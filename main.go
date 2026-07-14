// Command reconciler is the pull-based recovery service: it SSHes to hosts whose
// runs are parked in 'reconciling' and harvests their WAL, so a job that finished
// on the host (but whose push never reached the control plane) is recovered to
// its true outcome instead of being falsely failed. See host_side_runner_spec.md
// §5 and services/reconciler/core.
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/praetordev/crypto"
	"github.com/praetordev/db"
	"github.com/praetordev/env"
	"github.com/praetordev/metrics"
	"github.com/praetordev/plog"
	core "github.com/praetordev/reconciler/core"
)

func main() {
	plog.Configure("reconciler")
	log.Println("Starting Reconciler Service...")

	// Credentials are decrypted to reconstruct the SSH identity for a past run.
	if err := crypto.ValidateSecrets(false); err != nil {
		log.Fatalf("secrets misconfigured: %v", err)
	}

	database, err := db.Connect(env.String("DATABASE_URL", db.DefaultDSN))
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer database.Close()

	// The run event/log endpoints live on the ingestion service (the same target
	// the host-runner pushes to), not the API. Prefer INGESTION_URL, fall back to
	// API_URL, then the in-cluster default.
	apiURL := env.String("INGESTION_URL", env.String("API_URL", "http://ingestion:8081"))

	interval := durationEnv("RECONCILE_INTERVAL")
	if interval <= 0 {
		interval = 30 * time.Second
	}

	metrics.Serve("")

	rec := core.NewReconciler(database, interval, apiURL)
	// Horizontal-scale + tiering tunables (all optional; NewReconciler sets sane
	// defaults). Batch/Concurrency set per-replica throughput; ClaimTTL leases a
	// claimed run so K replicas never double-harvest; ColdAfter/ColdBackoff move
	// probably-dead hosts to a cheap sweep. See services/reconciler/core.
	if n := env.Int("RECONCILE_BATCH", 0); n > 0 {
		rec.Batch = n
	}
	if n := env.Int("RECONCILE_CONCURRENCY", 0); n > 0 {
		rec.Concurrency = n
	}
	if n := env.Int("RECONCILE_COLD_AFTER_ATTEMPTS", -1); n >= 0 {
		rec.ColdAfterAttempts = n
	}
	if d := durationEnv("RECONCILE_CLAIM_TTL"); d > 0 {
		rec.ClaimTTL = d
	}
	if d := durationEnv("RECONCILE_COLD_BACKOFF"); d > 0 {
		rec.ColdBackoff = d
	}
	// Harvest POSTs hit ingestion's run-scoped endpoints, which require auth. The
	// internal token is accepted for any run (see cmd/ingestion runTokenAuth); without
	// it every harvest is rejected 401 and recovered runs are falsely declared lost.
	if tok := env.String("PRAETOR_INTERNAL_TOKEN", ""); tok != "" {
		rec.InternalToken = tok
	} else {
		log.Println("WARNING: PRAETOR_INTERNAL_TOKEN unset — WAL harvest POSTs will be rejected 401 and runs cannot be recovered")
	}
	go rec.Start()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	log.Println("Shutting down...")
	rec.Stop()
}

// durationEnv parses key as a Go duration (e.g. "30s", "3m", "1h"); returns 0 if
// unset or unparseable, letting the caller apply its own default.
func durationEnv(key string) time.Duration {
	if v := env.String(key, ""); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return 0
}
