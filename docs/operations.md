# Operating Patchweft

Version 0.1 keeps its store on one Linux host and a local filesystem: SQLite WAL,
Git object storage and supervised native processes. Jobs run on that host and,
through [executors](#executors), on other Linux hosts. `/proc`, Linux
child-subreaper support, `renameat2`, `/dev/urandom` and `/usr/bin/git` are
required on every executing host. Do not put the state directory or an executor's
work directory on a network filesystem.

## Ownership and resources

Run under a dedicated Unix account for trusted code. Local CLI users with access
to the state directory have administrative access. API project/profile/role
permissions constrain remote application operations; they do not isolate executed
code from the worker account or other files that account can read.

Initialize the local executor's slot and memory budget. Admission is fair between
owners across all executors; `priority OWNER N`
admits a higher-priority owner's work first (for example a landing-ready merge train)
and `0` clears it. Priority orders admission only: it never preempts running work or
changes action identity. A higher-priority owner's job that waits only on a running dependency
(every dependency passed or running) holds the capacity it will need on each executor that may
run it: a lower-priority job is admitted only if it fits beside the demand of the priorities
above its own, counting the host's running work but not the dependencies being waited on, whose
capacity frees first. Every executor that may run a waiting job holds its full demand, so for a
whole train the fleet keeps that capacity until its prepare passes. The hold ends when the
dependency passes, fails, is interrupted or stopped, or nobody still wants the job. While a hold
keeps a lower-priority head back, the executor's record shows `holding` (priority, slots,
memory), with `executor.held` and `executor.hold_released` events. Jobs hold reservations until their supervised
processes and declared cleanup finish. A coordinator exit does not imply that
its jobs have stopped.

Private working directories prevent accidental workspace sharing, but are not
a hostile-code sandbox. Native slot and memory declarations do not impose
process limits. Placement by locality or cache warmth, replication and per-tenant
latency guarantees are not implemented.

## Executors

Each executor runs one coordinator, which launches one supervisor per attempt.
`worker` is the coordinator of the `local` executor on the store's host; `executor`
runs a remote one, which reaches the store only through the API. Both claim, renew,
complete and verify the same way. A second coordinator for the same executor
waits as a standby until the first exits.

Register a remote executor as an API principal and an executor record of the same
name. Its grant needs `read` and `execute` and names the projects and profiles it
may run; `local` is reserved:

```bash
patchweft --state /private/patchweft grant --policy mindy.policy.json \
  --credential-file /private/mindy.credential
patchweft --state /private/patchweft executor-set mindy --slots 4 --memory-mb 12288 \
  --allow-job test --allow-job lint
patchweft executor --endpoint https://api.example:7412 \
  --credential-file /private/mindy.credential --work /srv/patchweft-work
```

Use one credential file per executor host. Jobs on that host run as the
coordinator's account and can read it.

Each executor has its own budget: `executor-set local` writes the store's budget,
a remote executor's lives on its record. Remote holds never consume the local
budget, and the reverse. Memory admitted is the smaller of the configured value
and the cgroup `memory.high`/`memory.max` limit its coordinator reports. A job
that needs more than an executor's total budget is left to other executors.
Fair share and owner priority are global.

Placement is configuration read at claim time; it is never part of job identity,
action keys or evidence class. `--allow-job` and `--deny-job` replace their lists
and a denial wins. A remote executor claims nothing until jobs are placed on it
by name; the local executor admits every job unless given an allowlist. Keep jobs
that need the API host's network setup, such as browser tests, on `local`.

Profile identity binds runner bytes, declared tool paths and bytes and platform.
Install the identical binary and tools at the same absolute paths on every host.
A coordinator verifies each profile before claiming its work and again before
launching; a profile that does not match is refused and listed under
`observed.refused` by `patchweft executors`, with `verified`, `last_seen`, the
reported memory limit and free disk.

Before registering and before every claim, a coordinator checks the TCP listeners jobs can reach:
they run natively, so they reach every TCP listener on `127.0.0.0/8` or `::1`, and every listener
on a wildcard address (`0.0.0.0`, `::`, `::ffff:0.0.0.0`) at 127.0.0.1 with a loopback source,
whatever a Docker daemon exposes to containers. UDP is not checked. A job also reaches a listener
bound to one of the host's own addresses (a tailnet or LAN address) over `lo`; keep such services
off executor hosts, except ones that authenticate every caller, such as tailscaled's peerapi on the
tailnet address. Under WSL (mirrored networking) the Windows host's listeners can answer at
127.0.0.1 without appearing in Linux's tables, and the mode can't be ruled out from inside, so the
coordinator refuses all work on any WSL kernel: run executors in a Linux VM. A host firewall does
not filter `lo`, so a service that binds a wildcard and trusts loopback is reachable from every job. Each such listener must be in
`--loopback-allow PORT[,PORT…]`, unless the rootless Docker daemon's own `rootlesskit` (or a
process under it) holds it, those being the ports it publishes for job containers, or one of
the executor's own attempts does: a listener held by a process descending from a live supervisor
in its ledger is the job's own, not a host service. Ownership is looked up again whenever the
live supervisors change, so a listener left behind by a dead supervisor's orphan refuses. Otherwise the
executor refuses all work and its record shows `refused: {"*": "… listener … not allowlisted"}`.
Both `executor` and `worker` check by default; `--no-loopback-check` skips it, only for a host
whose every reachable service may be reached by jobs. Each executor's record shows how it checks
(`observed.loopback`: `off` or the allowlist). Run `ss -ltnH` for the host census, wildcard rows
included.

Attempts hold leases: 300 seconds by default for a remote executor and 3600 seconds
for `local`, whose supervisors keep running while its worker is stopped
(`executor-set --lease-seconds`). Stopping the local worker for longer than its
lease expires its running jobs, so drain it first for longer maintenance.
Leases are renewed every second by `worker` and every ten seconds by `executor`,
and never less often than a third of the lease. A coordinator
that cannot renew stops its jobs once the lease minus `max(2 × heartbeat,
lease / 5)`, at most half the lease, has passed since its last renewal, before the
API could expire it.
Only `serve` expires leases, and not within one lease of its own start. An
expired attempt is interrupted and stays undrained: its capacity is held until its
coordinator reports the drain or an operator confirms it. A coordinator whose
attempt was fenced stops the job, runs cleanup and may still report the drain
under its lease; it never records the result. A claim whose response was lost is
not lost with it: the coordinator keeps the claim's key in `pending-claim` and
sends it again first. If the lease lapsed meanwhile and the coordinator's ledger
never held the attempt, nothing of it ran on that host and the coordinator says
so; the store then drains the attempt, provided it never started. A lapsed lease
gives no verdict, so either report queues the action again, unless the late drain
report carries one: a completed command's exit, other than a stop the store counts as
an interruption, is never recorded and leaves the action interrupted for an operator.
Requeues share one budget with attempts that never started: three since the
action's last `retry`, after which the action stays interrupted for an operator. An
action nobody requests any more is canceled instead. Until a drain is reported or
confirmed, the job's entry in `status CANDIDATE` and in the `candidate` query
carries `awaiting_drain`, whether the job is interrupted, failed or canceled: the
attempt, the executor holding it, its `lease_expires`, and `self_heals`, true when
nothing of the attempt was recorded, so its coordinator's own report can still
settle it.

A remote executor's work directory holds `lock`, `pending-claim`, `ledger/`,
`attempts/`, `repos/` (bare mirrors), `cas/` (read-only artifacts by SHA-256),
`retained/` and `tmp/`. An acknowledged, drained attempt directory is deleted;
fenced or undrained ones move to `retained/` and are deleted after
`--retain-hours` (72). Artifacts it produced or downloaded stay in `cas/` up to
`--cas-max-gb` (20), least recently used first; inputs of attempts not yet
started are kept. Claims wait while less than `--min-free-gb` (20) is free after
that reclaim. A local attempt's directory in the store is its evidence (`output.log`,
`cleanup.log`, `supervisor.log`, `artifacts/`, `artifacts.json`) and is kept; once its report
is recorded and it has drained, its job trees (`work/`, `scratch/`, `home/`) are deleted, and
`worker` sweeps any drained attempt that still has them when it starts. Logs of a
remote attempt are available only after it completes.

## Failure and recovery

`cancel ACTION_ID --owner NAME` detaches that owner's interests. The last
requester causes a running action to stop. A new requester cannot join the
stopping attempt. Accepted work outlives API or MCP disconnection.

`recover` reconciles the local executor while no `worker` runs: it publishes
finished results and reports dead supervisors. A supervisor that died before its
command started is drained automatically. An attempt whose lease lapsed is drained
without an operator when its coordinator reports the drain or reports that it never
recorded the claim (see [executors](#executors)). Uncertain process or
external-resource cleanup keeps reservations held, and so does a lapsed lease
nothing reports: the coordinator or its work directory is gone, its binary predates
these reports, or the job had started and its cleanup is uncertain. The job's
`awaiting_drain` names the attempt and its executor; `self_heals: false` means a
report was recorded undrained and only an operator can settle it. Only after
inspecting and draining those resources on the attempt's host should an
authorized operator use
`recover --confirm-drained ATTEMPT_ID`. That command records the operator's
assertion about that host's processes and external resources; it cannot establish
the assertion. It keeps a failed, canceled or passed attempt's state and result
and refuses attempts still held by a coordinator.

Before confirming drain, inspect the adapter's durable resource records and
verify that every owned resource is absent, including resources created before
startup failed and dependencies left after a parent was removed. Retry the
adapter's cleanup when needed. Preserve the records until verification finishes;
an exited command or a missing parent alone is insufficient. See the
[external resource hook contract](profiles.md#external-resource-hooks).

After a supervised command times out, its attempt records `reason: "timeout"`
and exit code 124 in the command evidence. Its combined stdout/stderr remains
in `attempts/ATTEMPT_ID/output.log`, with `log_sha256` in the result. A cleanup
failure can change the attempt's final exit code and holds its reservation.
Retain the failed attempt's log and evidence when retrying; a successful retry
is a separate observation. Adapter subprocess wrappers should also preserve
partial output on timeout or startup failure, rather than replacing it with
only an exception message.

`retry CANDIDATE_ID` retains successful obligations and retries failed,
interrupted or canceled obligations. Publish a new revision when source changes.
`resolve REPAIR_ID --candidate PASSING_CANDIDATE_ID` records repair attribution;
it does not establish that the new candidate causally fixed the original defect.

## Review and policy

Frontier promotion requires a certifying profile, passing obligations, an
exact-revision review, completed and drained evidence, ancestry from the previous
frontier, and a matching expected frontier. Promotion also recomputes each
obligation's job identity, dependency wiring, inputs and action key from the
certifying profile, and checks a retained pass against that profile's timeout.
Exploratory work never serves certifying candidates.

The certificate's `profile` is the certifying policy that accepted these job
identities, not a claim that every job ran under it. The certificate also names
the predecessor candidate when passes were retained. Each receipt records the
executing profile, job identity, action key, route (`own`, `joined`, `retained`,
or `legacy` for actions recorded before job identity), observation start and
end, and attempt counts. Read it with `frontier PROJECT_ID` or the `frontier`
query.

Review records are assertions, not authenticated signatures or proof of reviewer
independence. A certifying profile is an operator's policy decision. Registering
a newer profile does not automatically retire older certifying profiles or enforce
one current project policy. Operators must constrain eligible profiles through
their permissions and promotion workflow.

## Backups and upgrades

Stop accepting work, finish or explicitly drain active attempts and external
resources, then stop workers and the API. Copy the entire state directory while
quiescent: database and journal files, Git objects, artifacts, workspaces and
receipts belong together. Retain a copy of the exact runner binary and profile
toolchain. Restore into a separate directory and inspect it before resuming.

There is no automatic database migration or rolling-upgrade protocol. Back up
before changing binaries. Profile and job identity bind runner bytes, so
re-register profiles after changing the runner. Do not mix versions against a
live store. Host power-loss recovery and backup/restore under arbitrary failures
have not been established by the included tests.

To upgrade:

1. Stop accepting work. Queued actions of profiles registered with the old
   binary stay queued after the change and are listed as refused by
   `patchweft executors`; cancel or resubmit them under the new profile IDs.
2. Drain active attempts and external resources. `worker` refuses to start
   while attempts started by the previous binary are held.
3. Back up the whole state directory.
4. Upgrade the server, every executor host and every remote client binary
   together. Clients and the server must agree on the command format.
5. Re-register profiles and grant the new profile IDs.
6. Resume.

Stored rows are not rewritten and no key migration runs. Actions recorded before
job identity keep working by ID for status, delivery and dependencies, and
promotion checks them against their original key. They are never shared or
retained, so the first run of each profile after an upgrade executes every job.

Logs, revisions, events, attempts and artifacts are retained in the store.
`serve` deletes uploads nobody consumed within 24 hours; other automatic retention
and garbage collection are not implemented. Monitor disk space. Deleting files
inside the state store can invalidate evidence or replay; there is no supported
selective pruning command.

## Connections and limits

Use the [connection guide](connections.md) for private credential files, TLS,
roles, receipts and event cursors. The API and `worker` must share the store's
host, state directory and binary; executors share the binary. Non-loopback HTTP
listeners require TLS.

Source downloads carry the reachable history a client lacks: an executor sends
its mirror's newest revisions as `have`, so each new commit is fetched once per
project and host. Bundles are limited to 1 GiB. Incremental uploads are possible
when the server already has the prerequisites. Large histories, Git packing,
disk pressure and network bandwidth can dominate request time.

The API has bounded calls, blocking operations, streams, transfers and pending
actions. Each artifact or log file is limited to 1 GiB and an attempt's outputs to
4 GiB on every host. History growth has no automatic quota.
Metrics are process-local and reset on restart. See the connection guide for
the exact bounds and timeout behavior.
