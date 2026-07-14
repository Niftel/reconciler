# reconciler

Praetor's **reconciler** service — the horizontally-scalable recovery worker that
makes sure no job is lost when a component fails mid-flight.

It runs the pull-side recovery path (complementing the host-runner's push-drain):
it claims stale/interrupted runs with `SKIP LOCKED` so multiple replicas share the
work without stepping on each other, harvests their on-disk WAL over SSH to
reconstruct events an outage would otherwise have dropped, and drives each run to a
terminal state. Losing the ingestion path never loses a job.

It is a leaf deployable: nothing imports it. It depends only on the shared
`praetordev/*` libraries (`db`, `events`, `hostconn`, `credentials`, `crypto`,
`metrics`, `env`, `plog`).

## Layout

```
main.go     entrypoint
core/       claim loop, WAL harvest, reconciliation, metrics
```

## Build the image

```
docker build -t praetor-reconciler:latest .
```

Stable image name (`praetor-reconciler`) so the Helm chart and k3d/kind load step
are unaffected by the repo split. uid 1000 matches the executor so the shared SSH
known_hosts volume is read/writable by both.

## Tests

The DB/SSH-backed integration tests are gated on their env vars and skip without
them.

```
go test ./...
```
