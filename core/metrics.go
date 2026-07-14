package core

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ReconcileOutcomes counts what happened to a reconciled run, by outcome:
	// recovered_successful, recovered_failed, lost, still_running.
	ReconcileOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "praetor_reconciler_outcomes_total",
		Help: "Reconciliation outcomes by type.",
	}, []string{"outcome"})

	// ReconcileEventsProjected counts events pulled from host WALs and projected.
	ReconcileEventsProjected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "praetor_reconciler_events_projected_total",
		Help: "Total events projected from pulled host WALs.",
	})

	// ReconcileChunksProjected counts stdout chunks pulled and uploaded.
	ReconcileChunksProjected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "praetor_reconciler_log_chunks_projected_total",
		Help: "Total stdout log chunks projected from pulled host logs.",
	})

	// ReconcileAttempts counts unproductive attempts (backoff). A run is never given
	// up on from here — persistent failures only demote it to the cold sweep.
	ReconcileAttempts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "praetor_reconciler_attempts_total",
		Help: "Total reconcile attempts that made no progress (backed off).",
	})

	// ReconcileDemotions counts runs demoted from the hot sweep to the cold (hourly)
	// tier after ColdAfterAttempts consecutive failed probes — i.e. probably-dead
	// hosts moved off the fast path so they stop diluting recoverable-run throughput.
	ReconcileDemotions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "praetor_reconciler_demotions_total",
		Help: "Total runs demoted from the hot reconcile sweep to the cold tier.",
	})

	// ReconcileTick measures the wall time of one reconciler tick.
	ReconcileTick = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "praetor_reconciler_tick_duration_seconds",
		Help:    "Duration of one reconciler tick.",
		Buckets: prometheus.DefBuckets,
	})
)
