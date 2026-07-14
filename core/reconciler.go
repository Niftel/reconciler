// Package core implements pull-based reconciliation: harvesting a run's WAL from
// the host it ran on when the host-runner's push never reached the control plane
// (host unreachable at sync time, or the control plane was down for the whole
// run). It SSHes to the host, reads status.json + events.jsonl + stdout.log, and
// re-feeds them through the same ingestion endpoints a push uses, so projection
// is idempotent (consumer ON CONFLICT (execution_run_id, seq)). See
// host_side_runner_spec.md §5.
package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/praetordev/credentials"
	"github.com/praetordev/events"
	"github.com/praetordev/hostconn"
	"github.com/praetordev/plog"
	"golang.org/x/crypto/ssh"
)

// logger is the reconciler package component logger (handler installed by pkg/plog).
var logger = plog.New("reconciler")

// maxLogChunk matches the host-runner's LogSyncer so pulled stdout is chunked the
// same way; combined with a byte offset from already-stored chunks this yields
// gap-free, non-overlapping chunk seqs.
const maxLogChunk = 256 * 1024

// Reconciler periodically resolves runs parked in 'reconciling'.
type Reconciler struct {
	DB       *sqlx.DB
	APIURL   string
	Interval time.Duration
	Client   *http.Client

	// InternalToken authenticates the harvest POSTs to ingestion's run-scoped
	// endpoints (events/logs/heartbeat live behind runTokenAuth, which accepts the
	// internal token; see cmd/ingestion/main.go). Without it every harvest 401s and
	// the WAL never lands — the exact bug that made recovered runs look "lost".
	InternalToken string

	Batch      int           // candidates claimed per tick
	MaxBackoff time.Duration // cap on the hot-tier retry interval

	// Concurrency bounds how many claimed candidates a single replica harvests in
	// parallel within a tick. Harvest is network-bound (SSH RTT + sudo cat + chunked
	// POSTs), so this multiplies throughput without meaningful CPU cost.
	Concurrency int

	// ClaimTTL is how far forward a claim pushes reconcile_after, leasing the run to
	// this replica. It must exceed the worst-case time to harvest one batch wave; if
	// a pod dies mid-harvest the lease simply lapses and the run comes due again, so
	// crash recovery costs no extra code. See tick() / claim().
	ClaimTTL time.Duration

	// ColdAfterAttempts is the consecutive-failure count at which a run is demoted
	// from the 30s hot sweep to the ColdBackoff cadence, so a permanently-unreachable
	// host stops diluting throughput for recoverable runs. Demotion is a cadence
	// change only — it NEVER declares a run lost. See reschedule().
	ColdAfterAttempts int
	ColdBackoff       time.Duration

	stop chan struct{}
}

func NewReconciler(db *sqlx.DB, interval time.Duration, apiURL string) *Reconciler {
	return &Reconciler{
		DB:                db,
		APIURL:            apiURL,
		Interval:          interval,
		Client:            &http.Client{Timeout: 15 * time.Second},
		Batch:             10,
		MaxBackoff:        5 * time.Minute,
		Concurrency:       5,
		ClaimTTL:          3 * time.Minute,
		ColdAfterAttempts: 10,
		ColdBackoff:       time.Hour,
		stop:              make(chan struct{}),
	}
}

func (r *Reconciler) Start() {
	logger.Info("reconciler started", "interval", r.Interval, "api", r.APIURL,
		"batch", r.Batch, "concurrency", r.Concurrency, "claim_ttl", r.ClaimTTL)
	// Re-arm with a fresh jittered interval each tick so K replicas don't fall into
	// lockstep and contend on the same candidates every cycle. SKIP LOCKED already
	// makes a collision harmless; the jitter just spreads the DB load.
	timer := time.NewTimer(r.jitter())
	defer timer.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-timer.C:
			r.tick()
			timer.Reset(r.jitter())
		}
	}
}

// jitter returns the base interval plus up to ~15% random spread.
func (r *Reconciler) jitter() time.Duration {
	if r.Interval <= 0 {
		return 30 * time.Second
	}
	return r.Interval + time.Duration(rand.Int63n(int64(r.Interval/6)+1))
}

func (r *Reconciler) Stop() { close(r.stop) }

// candidate is a run due for reconciliation plus everything needed to reach its
// host — resolved from the snapshotted runner_host_id and the job's credential.
type candidate struct {
	RunID        uuid.UUID `db:"id"`
	UnifiedJobID int64     `db:"unified_job_id"`
	PersistedSeq int64     `db:"persisted_event_seq"`
	Attempts     int       `db:"reconcile_attempts"`
	HostName     string    `db:"host_name"`
	HostVars     []byte    `db:"host_vars"`
	CredentialID *int64    `db:"credential_id"`
}

func (r *Reconciler) tick() {
	defer func(start time.Time) { ReconcileTick.Observe(time.Since(start).Seconds()) }(time.Now())
	ctx := context.Background()

	ids, err := r.claim(ctx)
	if err != nil {
		logger.Error("claim query failed", "err", err)
		return
	}
	if len(ids) == 0 {
		return
	}
	cs, err := r.hydrate(ctx, ids)
	if err != nil {
		logger.Error("hydrate candidates failed", "err", err)
		return
	}

	// Harvest the claimed batch in parallel, bounded by Concurrency. Each processRun
	// uses its own SSH client and the shared (goroutine-safe) sqlx pool + http.Client,
	// so no per-run state is shared. The tick stays synchronous (wg.Wait) — the ticker
	// cadence is the natural re-entry guard.
	sem := make(chan struct{}, r.Concurrency)
	var wg sync.WaitGroup
	for _, c := range cs {
		wg.Add(1)
		sem <- struct{}{}
		go func(c candidate) {
			defer wg.Done()
			defer func() { <-sem }()
			// Bound a single harvest so a wedged run can't be held past its lease; the
			// deadline sits just inside ClaimTTL so another replica may safely re-claim.
			rctx, cancel := context.WithTimeout(ctx, r.ClaimTTL-15*time.Second)
			defer cancel()
			r.processRun(rctx, c)
		}(c)
	}
	wg.Wait()
}

// claim atomically leases up to Batch due 'reconciling' runs to THIS replica by
// pushing their reconcile_after forward by ClaimTTL under FOR UPDATE SKIP LOCKED,
// and returns the claimed run ids. Because reconcile_after is the same cell the
// candidate scan filters on, the claim itself is the lease: a claimed run is
// invisible to every other replica until it comes due again (either sooner, when
// processRun writes its verdict, or at TTL lapse if this pod dies mid-harvest).
// Hot-tier runs are ordered ahead of cold so a due cold run can never starve the
// hot set. FOR UPDATE OF er confines the row locks to execution_runs (not hosts).
func (r *Reconciler) claim(ctx context.Context) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	err := r.DB.SelectContext(ctx, &ids, `
		WITH due AS (
			SELECT er.id
			FROM execution_runs er
			JOIN hosts h ON h.id = er.runner_host_id
			WHERE er.state = 'reconciling'
			  AND (er.reconcile_after IS NULL OR er.reconcile_after <= now())
			ORDER BY (er.reconcile_tier = 'hot') DESC, er.reconcile_after NULLS FIRST
			LIMIT $1
			FOR UPDATE OF er SKIP LOCKED
		)
		UPDATE execution_runs er
		SET reconcile_after = now() + ($2 || ' seconds')::interval
		FROM due
		WHERE er.id = due.id
		RETURNING er.id`, r.Batch, strconv.Itoa(int(r.ClaimTTL.Seconds())))
	return ids, err
}

// hydrate loads the full candidate rows for a set of claimed ids, resolving the
// host and the job's credential needed to reach it. Ordering is irrelevant here —
// the batch is already claimed; claim() applied the hot-before-cold priority.
func (r *Reconciler) hydrate(ctx context.Context, ids []uuid.UUID) ([]candidate, error) {
	var cs []candidate
	err := r.DB.SelectContext(ctx, &cs, `
		SELECT er.id, er.unified_job_id, er.persisted_event_seq,
		       er.reconcile_attempts, h.name AS host_name, h.variables AS host_vars,
		       jt.credential_id
		FROM execution_runs er
		JOIN hosts h ON h.id = er.runner_host_id
		JOIN unified_jobs uj ON uj.id = er.unified_job_id
		LEFT JOIN job_templates jt ON jt.unified_job_template_id = uj.unified_job_template_id
		WHERE er.id = ANY($1)`, pq.Array(ids))
	return cs, err
}

func (r *Reconciler) processRun(ctx context.Context, c candidate) {
	client, sudo, err := r.connect(ctx, c)
	if err != nil {
		// Host unreachable is transient by assumption — the run may still hold the
		// authoritative WAL. Keep probing; never declare it lost from here.
		r.reschedule(ctx, c, "connect: "+err.Error())
		return
	}
	defer client.Close()

	jobDir := "/var/lib/praetor/jobs/" + c.RunID.String()

	// Host reachable but the job directory is gone (host rebooted and lost its
	// WAL): the run is genuinely unrecoverable (spec §5.3).
	if out, _ := hostconn.Run(client, "test -d "+hostconn.Quote(jobDir)+" && echo yes || echo no"); strings.TrimSpace(out) == "no" {
		r.markLost(ctx, c, "job directory gone on host")
		return
	}

	// Read the terminal marker first so we know whether the job is done.
	statusRaw, _ := hostconn.Run(client, sudo+"cat "+hostconn.Quote(jobDir+"/status.json")+" 2>/dev/null")
	var st struct {
		State       string     `json:"state"`
		MaxSeq      int64      `json:"max_seq"`
		CompletedAt *time.Time `json:"completed_at"`
	}
	hasStatus := json.Unmarshal([]byte(strings.TrimSpace(statusRaw)), &st) == nil && st.State != ""

	// Project any events the control plane hasn't seen, then the log tail. On a
	// projection error we back off and retry rather than advancing state.
	newSeq, err := r.projectEvents(ctx, client, sudo, jobDir, c)
	if err != nil {
		// Control-plane-side failure (ingestion down / 401): the WAL is intact on the
		// host, we just couldn't deliver it. Keep the run in 'reconciling' and retry —
		// this is what makes an ingestion outage of ANY length non-fatal.
		r.reschedule(ctx, c, "project events: "+err.Error())
		return
	}
	if err := r.projectLogs(ctx, client, sudo, jobDir, c.RunID); err != nil {
		logger.Warn("run log projection failed (non-fatal)", "run_id", c.RunID, "err", err)
	}

	if hasStatus && isTerminal(st.State) {
		r.finalize(ctx, c, st.State, st.MaxSeq, st.CompletedAt)
		logger.Info("run recovered", "run_id", c.RunID, "state", st.State, "max_seq", st.MaxSeq)
		return
	}

	// Not terminal: the job may still be running on the host. Keep monitoring so
	// long as we're making progress; give up only when a reachable host stops
	// producing new events for MaxAttempts consecutive checks (hung runner).
	if newSeq > c.PersistedSeq {
		r.advance(ctx, c, newSeq)
		return
	}
	// Reachable, job dir present, no new events, no terminal status: the play is
	// either still running (slow/quiet task) or the runner died mid-play. Either way
	// we don't know the outcome, so we keep probing rather than guessing lost. (A
	// reconciler-driven re-invocation of a dead runner is the follow-up that resolves
	// the died-mid-play case; until then this holds the run safely, never lies.)
	r.reschedule(ctx, c, "reachable but no progress and no terminal status")
}

// connect resolves the SSH identity for a run the same way the executor did (host
// vars overlaid by the job's Machine credential) and dials the host.
func (r *Reconciler) connect(ctx context.Context, c candidate) (*ssh.Client, string, error) {
	var vars map[string]interface{}
	_ = json.Unmarshal(c.HostVars, &vars)
	getVar := func(k string) string {
		if v, ok := vars[k]; ok {
			return fmt.Sprintf("%v", v)
		}
		return ""
	}

	var credEnv, credFiles map[string]string
	if c.CredentialID != nil {
		var err error
		credEnv, credFiles, err = credentials.ResolveInjectors(ctx, r.DB, *c.CredentialID)
		if err != nil {
			return nil, "", fmt.Errorf("resolve credential %d: %w", *c.CredentialID, err)
		}
	}

	addr := hostconn.FirstNonEmpty(getVar("ansible_host"), c.HostName)
	port := hostconn.FirstNonEmpty(getVar("ansible_port"), "22")
	user := hostconn.FirstNonEmpty(getVar("ansible_user"), credEnv["ANSIBLE_REMOTE_USER"])
	key := credFiles["ANSIBLE_PRIVATE_KEY_FILE"]
	if user == "" || key == "" {
		return nil, "", fmt.Errorf("no SSH user/key (assign a Machine credential to the template)")
	}
	if !strings.HasSuffix(key, "\n") {
		key += "\n"
	}
	client, err := hostconn.Dial(addr, port, user, []byte(key))
	if err != nil {
		return nil, "", err
	}
	// The host-runner writes the job dir as root; a non-root login reads it via sudo.
	sudo := ""
	if user != "root" {
		sudo = "sudo "
	}
	return client, sudo, nil
}

// projectEvents streams events.jsonl, POSTs every record with seq > the run's
// persisted_event_seq to the ingestion endpoint (idempotent downstream), and
// returns the highest seq observed in the log.
func (r *Reconciler) projectEvents(ctx context.Context, client *ssh.Client, sudo, jobDir string, c candidate) (int64, error) {
	raw, err := hostconn.Run(client, sudo+"cat "+hostconn.Quote(jobDir+"/events.jsonl")+" 2>/dev/null")
	if err != nil {
		return c.PersistedSeq, nil // no WAL yet is not an error
	}
	batch, maxSeq := filterNewEvents(raw, c.PersistedSeq)
	if len(batch) == 0 {
		return maxSeq, nil
	}
	if err := r.postJSON(fmt.Sprintf("%s/api/v1/runs/%s/events", r.APIURL, c.RunID), batch); err != nil {
		return c.PersistedSeq, err
	}
	ReconcileEventsProjected.Add(float64(len(batch)))
	return maxSeq, nil
}

// projectLogs pulls the not-yet-stored tail of stdout.log and uploads it as
// chunks continuing from the last stored (offset, seq).
func (r *Reconciler) projectLogs(ctx context.Context, client *ssh.Client, sudo, jobDir string, runID uuid.UUID) error {
	var stored struct {
		Bytes  int64 `db:"bytes"`
		MaxSeq int64 `db:"maxseq"`
	}
	if err := r.DB.GetContext(ctx, &stored,
		`SELECT COALESCE(SUM(byte_length),0) AS bytes, COALESCE(MAX(seq),-1) AS maxseq
		 FROM job_output_chunks WHERE execution_run_id = $1`, runID); err != nil {
		return err
	}
	// tail -c +N is 1-indexed: +(bytes+1) starts just after the stored bytes.
	tail, err := hostconn.Run(client, fmt.Sprintf("%stail -c +%d %s 2>/dev/null", sudo, stored.Bytes+1, hostconn.Quote(jobDir+"/stdout.log")))
	if err != nil || tail == "" {
		return nil
	}
	seq := stored.MaxSeq + 1
	for _, chunk := range splitChunks([]byte(tail), maxLogChunk) {
		url := fmt.Sprintf("%s/api/v1/runs/%s/logs?seq=%d", r.APIURL, runID, seq)
		if err := r.postBytes(url, chunk); err != nil {
			return err
		}
		ReconcileChunksProjected.Inc()
		seq++
	}
	return nil
}

// --- state transitions ---

// finalize records the authoritative terminal outcome from status.json. The
// consumer also transitions the run when it projects the terminal event; this
// makes the outcome deterministic even if events.jsonl lacked a terminal record,
// and always advances persisted_event_seq (spec §5.2 step 4).
func (r *Reconciler) finalize(ctx context.Context, c candidate, state string, maxSeq int64, completedAt *time.Time) {
	fin := time.Now()
	if completedAt != nil {
		fin = *completedAt
	}
	if _, err := r.DB.ExecContext(ctx, `
		UPDATE execution_runs SET
			persisted_event_seq = $2,
			state = CASE WHEN NOT run_is_terminal(state) THEN $3 ELSE state END,
			finished_at = COALESCE(finished_at, $4)
		WHERE id = $1`, c.RunID, maxSeq, state, fin); err != nil {
		logger.Error("finalize run failed", "run_id", c.RunID, "err", err)
		return
	}
	if _, err := r.DB.ExecContext(ctx, `
		UPDATE unified_jobs SET status = $2, finished_at = COALESCE(finished_at, $3)
		WHERE id = $1 AND NOT job_is_terminal(status) AND status <> 'error'`,
		c.UnifiedJobID, state, fin); err != nil {
		logger.Error("finalize job failed", "job_id", c.UnifiedJobID, "err", err)
	}
	ReconcileOutcomes.WithLabelValues("recovered_" + state).Inc()
}

// advance records progress on a still-running job: bump persisted_event_seq,
// reset the give-up counter, and re-check soon.
func (r *Reconciler) advance(ctx context.Context, c candidate, newSeq int64) {
	// Progress is evidence of life: reset the backoff counter AND promote back to the
	// hot tier, so a run that had been demoted to the cold sweep resumes 30s cadence.
	if _, err := r.DB.ExecContext(ctx, `
		UPDATE execution_runs
		SET persisted_event_seq = $2, reconcile_attempts = 0,
		    reconcile_tier = 'hot', reconcile_after = now() + interval '30 seconds'
		WHERE id = $1`, c.RunID, newSeq); err != nil {
		logger.Error("advance run failed", "run_id", c.RunID, "err", err)
	}
	ReconcileOutcomes.WithLabelValues("still_running").Inc()
}

// reschedule parks the run for another attempt with exponential-but-capped delay.
// It NEVER gives up: a run is declared lost only on positive proof the result is
// gone (the job dir missing on a reachable host — see processRun), never because a
// transient outage — of ANY duration — kept us from harvesting. A host that stays
// unreachable, or a control plane that stays down, simply keeps this run in
// 'reconciling' and re-probes at MaxBackoff until it can be resolved. attempts only
// drives the backoff curve now, not a give-up verdict.
func (r *Reconciler) reschedule(ctx context.Context, c candidate, reason string) {
	attempts := c.Attempts + 1
	ReconcileAttempts.Inc()
	delay, tier := reconcileSchedule(attempts, r.ColdAfterAttempts, r.MaxBackoff, r.ColdBackoff)
	if tier == "cold" {
		ReconcileDemotions.Inc()
	}
	logger.Info("run recovery retry scheduled", "run_id", c.RunID, "attempt", attempts, "delay", delay, "tier", tier, "reason", reason)
	if _, err := r.DB.ExecContext(ctx, `
		UPDATE execution_runs
		SET reconcile_attempts = $2,
		    reconcile_after = now() + ($3 || ' seconds')::interval,
		    reconcile_tier = $4
		WHERE id = $1`, c.RunID, attempts, strconv.Itoa(int(delay.Seconds())), tier); err != nil {
		logger.Error("reschedule run failed", "run_id", c.RunID, "err", err)
	}
}

// reconcileSchedule decides the next retry delay and tier for a run that just
// failed a probe. Below coldAfter consecutive failures it stays 'hot' on the
// exponential-capped backoff; at or beyond it, the run is demoted to 'cold' on the
// coldBackoff cadence so a permanently-unreachable host stops consuming hot-set
// throughput. This is purely a CADENCE decision — a run is never declared lost
// here (that requires positive proof; see markLost). A coldAfter of 0 disables
// demotion (everything stays hot).
func reconcileSchedule(attempts, coldAfter int, hotMax, coldBackoff time.Duration) (time.Duration, string) {
	if coldAfter > 0 && attempts >= coldAfter {
		return coldBackoff, "cold"
	}
	return backoffDelay(attempts, hotMax), "hot"
}

// markLost declares a run unrecoverable: host is gone or persistently unreachable.
func (r *Reconciler) markLost(ctx context.Context, c candidate, reason string) {
	logger.Warn("marking run lost", "run_id", c.RunID, "reason", reason)
	if _, err := r.DB.ExecContext(ctx, `
		UPDATE execution_runs SET state = 'lost', finished_at = now()
		WHERE id = $1 AND state NOT IN ('successful','failed','canceled')`, c.RunID); err != nil {
		logger.Error("markLost run failed", "run_id", c.RunID, "err", err)
	}
	if _, err := r.DB.ExecContext(ctx, `
		UPDATE unified_jobs SET status = 'error', finished_at = now()
		WHERE id = $1 AND status NOT IN ('successful','failed','canceled','error')`, c.UnifiedJobID); err != nil {
		logger.Error("markLost job failed", "job_id", c.UnifiedJobID, "err", err)
	}
	ReconcileOutcomes.WithLabelValues("lost").Inc()
}

// --- HTTP helpers (same endpoints the host-runner pushes to) ---

func (r *Reconciler) postJSON(url string, body interface{}) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return r.do(req)
}

func (r *Reconciler) postBytes(url string, data []byte) error {
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	return r.do(req)
}

func (r *Reconciler) do(req *http.Request) error {
	// The run-scoped ingestion endpoints require auth (runTokenAuth). The internal
	// token is accepted for any run, which is exactly what a control-plane harvester
	// needs. Without this header every POST is rejected 401.
	if r.InternalToken != "" {
		req.Header.Set("Authorization", "Bearer "+r.InternalToken)
	}
	resp, err := r.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ingestion returned status %d", resp.StatusCode)
	}
	return nil
}

// --- pure helpers (unit-tested) ---

// isTerminal reports whether a status.json state is a finished outcome. Must match
// the states the host-runner writes as terminal (main.go writeTerminal), including
// canceled — otherwise a canceled run is never finalized from its WAL.
func isTerminal(state string) bool {
	return state == "successful" || state == "failed" || state == "canceled"
}

// filterNewEvents parses an events.jsonl blob and returns the records with
// seq > persisted (to project) plus the highest seq seen across the whole log
// (to detect progress). Corrupt/partial lines are skipped, not fatal.
func filterNewEvents(raw string, persisted int64) (batch []events.JobEvent, maxSeq int64) {
	maxSeq = persisted
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var ev events.JobEvent
		if json.Unmarshal([]byte(line), &ev) != nil {
			continue
		}
		if ev.Seq > maxSeq {
			maxSeq = ev.Seq
		}
		if ev.Seq > persisted {
			batch = append(batch, ev)
		}
	}
	return batch, maxSeq
}

// splitChunks slices data into pieces of at most max bytes, preserving order and
// total length (so re-chunked stdout lines up byte-for-byte with stored chunks).
func splitChunks(data []byte, max int) [][]byte {
	var out [][]byte
	for len(data) > 0 {
		n := len(data)
		if n > max {
			n = max
		}
		out = append(out, data[:n])
		data = data[n:]
	}
	return out
}

// backoffDelay is an exponential retry delay (30s doubling) capped at max.
func backoffDelay(attempts int, max time.Duration) time.Duration {
	shift := attempts
	if shift > 4 {
		shift = 4
	}
	delay := time.Duration(1<<shift) * 30 * time.Second
	if delay > max {
		delay = max
	}
	return delay
}
