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
changes action identity. Among work of equal fair share (one owner's, or owners not yet
served), the job ahead of the longest chain of queued work goes first, then the oldest: a job's
rank is its expected occupancy plus the highest rank among the queued jobs waiting on it, so once
a train's prepare passes, its long shards are admitted before short ones queued earlier.
Expected occupancy is the median of the job's last 7 passes in its profile, from claim to result
on the store's clock; a pass whose job created `PATCHWEFT_VACUOUS` (it skipped its work) is not
counted. Passes recorded before a version that knows the marker count as before, so after that
upgrade a surface job's estimate keeps its older skip passes until its first pass that does its
work: the first train in which it applies pays for them once. While its own profile has none,
it is that of the most recently updated profile with the same definition name (lineage) in the
same project, so a re-registered profile inherits its predecessor's; other projects and other
names (a nightly beside a validation profile) never count. Each project, lineage and job keeps
the samples of its 3 most recently updated profiles.
A job with no history there either expects the median of what its profile's other queued jobs
are expected to take, so a new shard does not wait behind every known short job. The first
preparation after an upgrade seeds the samples from the passes already recorded.
Without history every rank is 0 and the oldest goes first. An executor drains for its head: a
head that does not fit yet keeps smaller jobs back until it does, and ranking makes a long job
the head more often.

A job queued behind a blocked head still starts beside it (backfill) when all of these hold:

- It is admitted on its own terms: it fits, beside the hold and within the reserve or the lent
  reserve.
- It is of the head's own priority.
- It is another owner's job, or the head's own owner's while no other owner has queued work of
  that priority that may run on the executor, whatever that work's dependencies: the claim moves
  its owner's fair share, and another owner's work could then pass the head. This is how a
  train's small jobs start beside its own blocked shard.
- It delays nothing ranked before it at that priority: whichever of the executor's running jobs
  end first, the head and then each job of its priority ranked ahead of it, in rank order, start
  just as soon as they would without it.
- No unverified job of that priority is ranked ahead of it, at most 12 jobs are held on the
  executor, no hold above the head's priority applies there (a hold lifts wherever its job does
  not land, and the head would then start at once), and its claim would not move the median a
  job without history is ranked by.

The owner test is per executor while fair share is global, so a backfill of the head's own owner's
job can move that owner behind another owner on a different executor. The guarantee covers the
jobs queued and the holds at the time of the claim: a train that arrives, or a hold that starts,
after a backfill can wait for the backfilled job, as for any lane admission there. On an executor
with a reserve, lane work backfilled within what the reserve leaves never takes the reserve, and
lane work backfilled into the lent reserve is bounded by the lending bound (below).

A higher-priority owner's job that waits only on a running dependency
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

The hold has nothing to hold for before a train is submitted, and priority never preempts, so
lane work claimed just before a train can keep its first job waiting. A standing reserve covers
that: `reserve EXECUTOR --slots N --memory-mb M` keeps part of an executor's capacity for owners
with priority above 0, and `--slots 0 --memory-mb 0` clears it. `patchweft executors` shows it as
`reserve`, and each change is an `executor.reserve` event. Between trains the reserve would sit
idle, so it is lent to short lane work (below).

Work no owner above 0 wants occupies the reserve only while it is lent (below): otherwise it is
admitted only if it fits, beside the other such work held there, within the capacity less the
reserve. Prioritized work running there, inside the reserve or beyond it, never counts against
what is left to priority 0; the whole capacity still bounds everything the executor runs, and a
lower-priority head must fit beside both the hold and the reserve. A head the reserve keeps back
drains the executor as one that does not fit yet does; it is not a hold and does not set
`holding`. Work of an owner at 0 or below that is larger than the capacity less the reserve is
left to other executors, as work larger than an executor is, so it never keeps that executor's
smaller work back; only short work that fits the reserve stays while the reserve is lent.

An executor's reserve is lent while no owner above 0 wants any work that is queued, running or
stopping, on any executor (no train rides), and no attempt for such an owner was claimed or ended
on that executor in the last 2 minutes. The pause keeps the reserve from being lent in the gap
between back-to-back trains. It is per executor: a train's last attempt on mindy does not keep
mauve's reserve from being lent if mauve ran none of that train's work in those 2 minutes. That is
enough for a reserve host every train touches, and an executor no train used lends at once. A job of an owner at 0 or below may then take the reserve, beside
the other lent work and up to the reserve's size, if it does not fit the capacity less the
reserve and its history holds at least two passes, every one under 10 minutes: a job with one
pass or none is never lent, nor one that mostly takes a minute but sometimes a quarter of an
hour. History is per profile and job, not per host, so a job quick on one host is lent on a
slower one too, and after a profile change it comes from the previous profile of the same name
(lineage) until the new one has passes of its own. Lent work counts against the reserve only,
never against what the reserve leaves, and its attempt says `"lent": true`. From the first
prioritized request nothing more is lent; lane work keeps what the reserve leaves while lent work
drains, and short work larger than that is again left to other executors.

Lent capacity is borrowed. Once a lent job has run 10 minutes while prioritized work on its
executor does not fit, and that work has been runnable as prioritized work for 10 seconds (since
the latest of its creation, its dependencies' passes and its owner's last priority change; a few
polls, so an executor with room takes it first and no lane job is stopped for work another
executor starts at once), the store stops the fewest lent jobs past that bound whose end makes room for it, the
oldest first among as few, and records `attempt.reclaimed` with the action, the
executor, how long it ran and the work it was stopped for. A lane whose job is reclaimed sees its
attempt stop and end `interrupted` with reason `reclaimed`: nothing failed and no repair opens.
The job is queued again within the same requeue budget as any interruption, and runs again like
any other queued lane job: within the lanes' share, or lent the reserve again once lending is
back on. A reclaimed job may be lent again, and stops after the requeue budget (three
reclaims or other interruptions, then it ends `interrupted` with reason `reclaimed` and waits for
an operator's `retry`). A train that arrives while the reserve is lent so waits
for lent work about 10 minutes at most, plus the stop and the job's cleanup; a remote executor
learns of the stop at its next renewal, and one that cannot be reached holds the capacity until
its lease lapses and its drain is reported or confirmed.

Why a job waits shows on `patchweft executors`. Each executor lists, under `waiting`, at most 32
queued jobs it may run but does not admit now, each with its action, owner and `reason`, and
`waiting_more` counts the ones left off the list:

- `verify`: the executor's head, whose profile (`profile`) the executor has not verified;
  nothing is admitted there until it has. After a profile is registered again, this is the first
  wait to expect.
- `drain`: the executor's head, which does not fit yet; the executor drains for it.
- `hold`: the head, kept back by a prioritized owner's hold (`priority`).
- `lanes-share`: larger than what the reserve leaves to lanes now, and not lent the reserve;
  `lending` names why: `no-history`, `too-few-passes` (fewer than two), `too-long` (a pass took
  10 minutes or more), `train-has-work`, `train-recent` (a train's attempt on that executor was
  claimed or ended in the last 2 minutes), `reserve-full` (what the reserve holds for lent work
  is taken) or `larger-than-reserve`.
- `behind`: queued behind the head (`head`) the executor drains for, holds or awaits verification
  of; the one job backfill would start beside the head is not listed.
- `capacity`: larger than the executor.

Each executor judges with its placement and the profiles it last reported verified or refused; a
grant narrower than every project and profile can keep more work from it. A job that every
executor lists is one nothing will run until the cause changes. `held` shows what runs there,
`holding` a hold above some priority, and the reserve `applied`, `lent` (the reserve is applied
and lent now) and `lent_held` (what lent work still holds). The lines lanes read in LANES.md come
with the cutover's lab PR (below).

A queued action counts as prioritized while any owner above 0 wants it, as it does for ordering,
and its claim is that owner's. A running attempt counts as prioritized while the owner it was
claimed for is above 0 and still wants it: work a lane claimed stays the lanes' even once a train
shares it, so a train that lets go of it later cannot leave lane work in the reserve.

Size the reserve, in each dimension, as the largest job a prioritized owner's work must start
there without waiting. For NG's merge train that is 8 slots, its `prepare`'s, and 9216 MiB, a
whole-tree Vitest shard's, which is larger in memory than `prepare`: on a 20-slot, 26624 MiB
local, 8192 MiB left train 95's second-wave shard waiting behind two lane shards. Set it again
when those declarations change.

The reserve must stay below the executor's capacity in both slots and memory, so other work
always keeps some room. An `executor-set` that would lower the capacity to the reserve or below
is refused until the reserve is lowered or cleared. A cgroup limit cannot be refused: while the
reported limit leaves the reserve no room below it, the reserve is not applied, rather than
strand other work, and `patchweft executors` shows it with `"applied": false`.

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
an interruption, or a result that could not be delivered is never recorded and
leaves the action interrupted for an operator. A drained report under a held lease
is judged the same way: an attempt that never started, a lost supervisor, or a
command stopped by an interruption (`supervisor-stop`, `partition`, `fenced`,
`revoked`) queues the action again, so a short partition heals like a long one. A
command that had already failed on its own before such a stop keeps its exit code
and is a failure, including one that exited before a SIGTERM reached its supervisor
(an executor deploy); the evidence records the stop as `stopped_after_exit`. Requeues
share one budget: three since the action's last `retry`, after which the action
stays interrupted for an operator. An action nobody requests any more is canceled
instead, whatever the report.

The store waits for those reports for a bounded time: two of the executor's leases
after expiry interrupted the attempt (10 minutes for a 300-second lease). The deadline
is fixed when the lease expires, from the lease the executor had then, and recorded as
the attempt's `heal_until`: changing the lease later never reopens or shortens it. Until then,
while no report of the attempt is recorded and a drain could still move its action
(requeue budget left, or nobody wants the work), the job may still heal itself: the candidate is `pending`, as while a job is queued or
running, so `wait` keeps waiting. A report that arrives in time settles the job as
above. Once the bound passes, once the coordinator's late report is refused for being
undrained (it never reports again), or at once when the budget is already spent and the work is
still wanted (any report could only leave the job interrupted), the candidate is
`failed` for an operator, and stays so: a
drain reported later only releases the capacity, and `retry` runs the job again.
Only time bounds the wait, since a coordinator learns it was fenced before it has
stopped its job and cleaned up. Until a drain is reported or confirmed, the job's
entry in `status CANDIDATE` and in the `candidate` query carries `awaiting_drain`,
whether the job is interrupted, failed or canceled: the attempt, the executor
holding it, its `lease_expires`, and `may_self_heal`, the store's own bounded
judgement that the job may still heal itself.

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
`awaiting_drain` names the attempt and its executor; `may_self_heal: false` means
only an operator can settle it (see [executors](#executors)). Only after
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
selective pruning command. Nothing prunes the store, on NG's hosts or elsewhere. A future pruner
is handed off and gated on its own, and may delete only an attempt's directory that is drained,
past its retention, and neither a dependency of a live action nor a reused result: `logs` reads
`attempts/<id>/output.log`, and a `local` claim reads its dependencies' artifacts from
`attempts/<producer>/artifacts`.

### Cutover of the lent reserve (lumenloop-ng#4204)

The change adds an optional `lent` key to attempt bodies, read as false where it is missing, and
two indexes on `attempts` and a small one on `events` (priority changes only) that the first open
after the upgrade builds. No table, queue format
or row changes. The reserve itself is operator state in the store, set with `patchweft reserve`
and recorded as an `executor.reserve` event; nothing in code or configuration sets it. In-flight
runs cannot cross the swap: `worker` refuses to start while the previous binary's attempts are
held, so drain first. The chief names the moment.

Cutover, as one window:

1. No train riding, after #4772, #4773 and #4779 land.
2. Stop accepting work and drain mauve, mindy and pw-robin.
3. Back up the whole state directory.
4. Install the one binary on the API server, on all three executors and on every remote client
   binary the lanes use, together.
5. Register the new profile (v28) and grant it.
6. Repoint the 16 lane configs. Cancel or resubmit anything still queued under v27, which
   `patchweft executors` lists as refused.
7. Run one proving train. It is cold, so every job executes.
8. Set mauve's reserve (its executor is `local`): `patchweft --state ~/state reserve --slots 8
   --memory-mb 18432 local`. Confirm that
   `patchweft executors` shows it with `"applied": true`: a reserve that meets the effective
   capacity, for example under a lower cgroup limit, is not applied.
9. Merge the cutover's lab PR: `pw-ng.py` reads an attempt that ended `interrupted` with reason
   `reclaimed` as HELD for a lane check, never FAIL (the lane did not cause it, and its job runs
   again), and LANES.md gains the lines on why a job waits (`waiting` on `executors`) and on
   reclaimed jobs.

What can go wrong: a missed v27 queued action sits refused; the reserve does not apply under the
cgroup limit; the cold proving train runs long; lending turns on in gaps during that cold window,
using v27's estimates by lineage.

Rollback:

1. Drain.
2. Reinstall the old binary everywhere. The old binary reads `"lent": true` bodies, and the
   drain leaves none held; the indexes stay and are harmless.
3. Repoint the 16 lane configs back to v27, which is still registered.
4. Cancel or resubmit work queued under v28.
5. Put mauve's reserve back to the stopgap: `patchweft --state ~/state reserve --slots 8
   --memory-mb 9216 local`.
   The old binary cannot lend, so 18432 MiB would bring back the 40 to 50 minute lane queueing.

What can go wrong: forgetting step 5, and v28 work left queued and refused. v28's samples stay
in `job_estimates`, and v27's own rows come first, so its estimates stay sane.

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

The API listener probes idle connections (60 s, then every 15 s, 4 probes) and, on Linux,
closes one whose data has gone unacknowledged for 2 minutes. That also resets a live client that
stops reading for more than 2 minutes; `events --follow` then reconnects from its last event.

The API has bounded calls, blocking operations, streams, transfers and pending
actions. Each artifact or log file is limited to 1 GiB and an attempt's outputs to
4 GiB on every host. History growth has no automatic quota.
Metrics are process-local and reset on restart. See the connection guide for
the exact bounds and timeout behavior.
