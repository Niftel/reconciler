package core

// Integration tests that exercise the reconciler's SQL against a REAL Postgres:
// the FOR UPDATE SKIP LOCKED claim (no double-harvest across replicas), the
// reconcile_after lease + expiry, hot-before-cold ordering, and the cold-tier
// demotion / hot promotion — including the load-bearing safety invariant that
// demotion NEVER marks a run lost. They are gated on TEST_DATABASE_URL so the
// normal `go test ./...` unit run skips them; run via scripts/reconciler-it.sh,
// which stands up a throwaway PG and applies every migration.

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func openTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set — skipping reconciler DB integration test")
	}
	db, err := sqlx.Connect("postgres", url)
	if err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	db.SetMaxOpenConns(16) // let concurrent claims actually run in parallel
	t.Cleanup(func() { db.Close() })
	return db
}

// base is the shared org→inventory→host chain a run needs to be reachable.
type base struct {
	orgID, invID, hostID int64
}

func seedBase(t *testing.T, db *sqlx.DB) base {
	t.Helper()
	suffix := uuid.NewString()[:8]
	var b base
	mustScan(t, db, `INSERT INTO organizations (name) VALUES ($1) RETURNING id`, &b.orgID, "rec-org-"+suffix)
	mustScan(t, db, `INSERT INTO inventories (organization_id, name) VALUES ($1,$2) RETURNING id`, &b.invID, b.orgID, "rec-inv-"+suffix)
	// A resolvable ansible_host keeps connect() from failing on address, though these
	// tests never actually SSH — they drive claim/reschedule/advance directly.
	mustScan(t, db, `INSERT INTO hosts (inventory_id, name, variables) VALUES ($1,$2,'{"ansible_host":"127.0.0.1"}'::jsonb) RETURNING id`, &b.hostID, b.invID, "rec-host-"+suffix)
	t.Cleanup(func() { db.Exec(`DELETE FROM organizations WHERE id=$1`, b.orgID) })
	return b
}

// seedRun inserts a unified_job + a 'reconciling' execution_run pinned to the base
// host, with the given tier/attempts and reconcile_after. Returns the run + job id.
func seedRun(t *testing.T, db *sqlx.DB, b base, tier string, after time.Time, attempts int) (uuid.UUID, int64) {
	t.Helper()
	var jobID int64
	mustScan(t, db, `INSERT INTO unified_jobs (name, status) VALUES ($1,'running') RETURNING id`, &jobID, "rec-job-"+uuid.NewString()[:8])
	var runID uuid.UUID
	mustScan(t, db, `
		INSERT INTO execution_runs
		  (unified_job_id, state, runner_host_id, reconcile_after, reconcile_tier, reconcile_attempts)
		VALUES ($1,'reconciling',$2,$3,$4,$5) RETURNING id`,
		&runID, jobID, b.hostID, after, tier, attempts)
	t.Cleanup(func() { db.Exec(`DELETE FROM unified_jobs WHERE id=$1`, jobID) })
	return runID, jobID
}

func mustScan(t *testing.T, db *sqlx.DB, q string, dest interface{}, args ...interface{}) {
	t.Helper()
	if err := db.QueryRow(q, args...).Scan(dest); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
}

type runRow struct {
	State          string    `db:"state"`
	Tier           string    `db:"reconcile_tier"`
	Attempts       int       `db:"reconcile_attempts"`
	PersistedSeq   int64     `db:"persisted_event_seq"`
	ReconcileAfter time.Time `db:"reconcile_after"`
}

func getRun(t *testing.T, db *sqlx.DB, id uuid.UUID) runRow {
	t.Helper()
	var r runRow
	if err := db.Get(&r, `SELECT state, reconcile_tier, reconcile_attempts, persisted_event_seq, reconcile_after
		FROM execution_runs WHERE id=$1`, id); err != nil {
		t.Fatalf("get run %s: %v", id, err)
	}
	return r
}

// TestClaim_SkipLocked_NoDoubleClaim is the headline HA guarantee: N due runs and
// G concurrent claimers (simulating G replicas ticking at once) must partition the
// set with ZERO overlap — no run is ever handed to two replicas. This is what makes
// reconciler.replicas > 1 safe; without SKIP LOCKED every replica would grab (and
// re-SSH) the same batch.
func TestClaim_SkipLocked_NoDoubleClaim(t *testing.T) {
	db := openTestDB(t)
	b := seedBase(t, db)

	const runs, claimers, batch = 30, 6, 5
	seeded := map[uuid.UUID]bool{}
	for i := 0; i < runs; i++ {
		id, _ := seedRun(t, db, b, "hot", time.Now().Add(-time.Minute), 0)
		seeded[id] = true
	}

	r := NewReconciler(db, time.Second, "")
	r.Batch = batch
	r.ClaimTTL = 5 * time.Minute

	var mu sync.Mutex
	claimedBy := map[uuid.UUID]int{} // run id -> how many claimers got it
	start := make(chan struct{})
	var wg sync.WaitGroup
	for c := 0; c < claimers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release all claimers together to force contention
			ids, err := r.claim(context.Background())
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			mu.Lock()
			for _, id := range ids {
				claimedBy[id]++
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	total := 0
	for id, n := range claimedBy {
		if n > 1 {
			t.Errorf("run %s claimed by %d claimers — SKIP LOCKED failed (double harvest)", id, n)
		}
		if !seeded[id] {
			t.Errorf("claimed unexpected run %s", id)
		}
		total++
	}
	// 6 claimers × batch 5 = 30 capacity for exactly 30 runs: all claimed, once each.
	if total != runs {
		t.Errorf("claimed %d distinct runs, want %d", total, runs)
	}
	// Every claimed run must now be leased into the future (invisible to re-claim).
	for id := range claimedBy {
		if got := getRun(t, db, id).ReconcileAfter; !got.After(time.Now()) {
			t.Errorf("run %s reconcile_after=%s not leased into the future", id, got)
		}
	}
}

// TestClaim_LeaseAndExpiry proves the lease semantics: a claimed run is invisible to
// a second claim until its lease lapses, and a pod that dies mid-harvest (lease
// expires without a verdict) makes the run claimable again — crash recovery for free.
func TestClaim_LeaseAndExpiry(t *testing.T) {
	db := openTestDB(t)
	b := seedBase(t, db)
	id, _ := seedRun(t, db, b, "hot", time.Now().Add(-time.Minute), 0)

	r := NewReconciler(db, time.Second, "")
	r.Batch = 10
	r.ClaimTTL = 5 * time.Minute

	first, err := r.claim(context.Background())
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if len(first) != 1 || first[0] != id {
		t.Fatalf("first claim = %v, want [%s]", first, id)
	}

	// Immediately re-claiming must NOT return it — it's leased 5m into the future.
	again, _ := r.claim(context.Background())
	if len(again) != 0 {
		t.Fatalf("re-claim returned %v, want empty (run is leased)", again)
	}

	// Simulate lease lapse (pod crashed mid-harvest): the run comes due again.
	if _, err := db.Exec(`UPDATE execution_runs SET reconcile_after = now() - interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	reclaim, _ := r.claim(context.Background())
	if len(reclaim) != 1 || reclaim[0] != id {
		t.Fatalf("after lease expiry claim = %v, want [%s]", reclaim, id)
	}
}

// TestClaim_HotBeforeCold proves cold-tier runs can never starve the hot set: even
// when a cold run is MORE overdue (would sort first by reconcile_after alone), the
// tier ordering fills the batch with the hot run first.
func TestClaim_HotBeforeCold(t *testing.T) {
	db := openTestDB(t)
	b := seedBase(t, db)
	// Cold is more overdue than hot — only the tier ordering should pick hot.
	coldID, _ := seedRun(t, db, b, "cold", time.Now().Add(-10*time.Minute), 20)
	hotID, _ := seedRun(t, db, b, "hot", time.Now().Add(-1*time.Minute), 1)

	r := NewReconciler(db, time.Second, "")
	r.Batch = 1 // only room for one — it must be the hot run
	r.ClaimTTL = 5 * time.Minute

	ids, err := r.claim(context.Background())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(ids) != 1 || ids[0] != hotID {
		t.Fatalf("claim = %v, want [%s] (hot before cold %s)", ids, hotID, coldID)
	}
}

// TestReschedule_DemotesToCold_NeverLost is the core safety guarantee against real
// SQL: a run that has failed ColdAfterAttempts consecutive probes is demoted to the
// cold tier on the hourly cadence — but stays 'reconciling', and its job stays
// non-error. Demotion is a CADENCE change, never a lost verdict.
func TestReschedule_DemotesToCold_NeverLost(t *testing.T) {
	db := openTestDB(t)
	b := seedBase(t, db)

	r := NewReconciler(db, time.Second, "")
	r.ColdAfterAttempts = 10
	r.ColdBackoff = time.Hour
	r.MaxBackoff = 5 * time.Minute

	// One below the threshold: reschedule takes it to attempt 10 → demote.
	runID, jobID := seedRun(t, db, b, "hot", time.Now(), 9)
	r.reschedule(context.Background(), candidate{RunID: runID, UnifiedJobID: jobID, Attempts: 9}, "unreachable")

	got := getRun(t, db, runID)
	if got.Tier != "cold" {
		t.Errorf("tier = %q, want cold", got.Tier)
	}
	if got.State != "reconciling" {
		t.Errorf("state = %q, want reconciling — demotion must NEVER mark lost", got.State)
	}
	if got.Attempts != 10 {
		t.Errorf("attempts = %d, want 10", got.Attempts)
	}
	if d := time.Until(got.ReconcileAfter); d < 50*time.Minute {
		t.Errorf("reconcile_after in %s, want ~1h (cold cadence)", d)
	}
	var jobStatus string
	mustScan(t, db, `SELECT status FROM unified_jobs WHERE id=$1`, &jobStatus, jobID)
	if jobStatus == "error" || jobStatus == "lost" {
		t.Errorf("job status = %q — demotion must not fail the job", jobStatus)
	}

	// Below the threshold a run stays hot on the exponential backoff.
	hotRun, hotJob := seedRun(t, db, b, "hot", time.Now(), 2)
	r.reschedule(context.Background(), candidate{RunID: hotRun, UnifiedJobID: hotJob, Attempts: 2}, "blip")
	if h := getRun(t, db, hotRun); h.Tier != "hot" {
		t.Errorf("below-threshold tier = %q, want hot", h.Tier)
	} else if d := time.Until(h.ReconcileAfter); d > 5*time.Minute+time.Second {
		t.Errorf("below-threshold reconcile_after in %s, want <= hot cap", d)
	}
}

// TestAdvance_PromotesToHot proves the revival path: progress on a demoted (cold)
// run resets the backoff and pulls it back into the 30s hot sweep.
func TestAdvance_PromotesToHot(t *testing.T) {
	db := openTestDB(t)
	b := seedBase(t, db)
	runID, jobID := seedRun(t, db, b, "cold", time.Now().Add(time.Hour), 20)

	r := NewReconciler(db, time.Second, "")
	r.advance(context.Background(), candidate{RunID: runID, UnifiedJobID: jobID, Attempts: 20}, 42)

	got := getRun(t, db, runID)
	if got.Tier != "hot" {
		t.Errorf("tier = %q, want hot (promoted on progress)", got.Tier)
	}
	if got.Attempts != 0 {
		t.Errorf("attempts = %d, want 0 (reset on progress)", got.Attempts)
	}
	if got.PersistedSeq != 42 {
		t.Errorf("persisted_event_seq = %d, want 42", got.PersistedSeq)
	}
	if d := time.Until(got.ReconcileAfter); d > 31*time.Second || d < 20*time.Second {
		t.Errorf("reconcile_after in %s, want ~30s (hot cadence)", d)
	}
}
