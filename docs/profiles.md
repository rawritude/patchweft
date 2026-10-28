# Execution profiles and shared preparation

A profile is an immutable, machine-specific definition of work. It names tool
files, exact argv, environment, dependencies, resource reservations, reports,
artifacts and optional cleanup. Registering it hashes the definition, declared
tools, runner binary and platform. Changed tools or a new runner require a new
registration. Use the returned profile ID in requests. The profile ID is the
handle for permissions and certification policy.

Every executing host must hold the identical runner and declared tools at the
same paths on the same platform. An executor whose tools, runner or platform
differ refuses the profile: its queued work stays queued instead of failing, and
`patchweft executors` lists the refusal. A drift found after an action was
claimed requeues it without a repair.

Each job also has a job identity, its recipe. A recipe hashes what the job's
process can observe from the profile: job name, command and cleanup argv, every
declared tool's path and bytes, the profile environment overlaid with the job
environment, report contract, artifacts, reuse policy, input list, exported
`slots`, the profile's `inputs_file` when set, the recipes of its dependencies,
runner binary and platform. It leaves
out `memory_mb`, `timeout_seconds`, `slots` with `export_slots: false`, `audit`,
and the profile's name, version and `certifying` flag; `certifying` decides the
evidence class instead. Sharing, completed reuse
and retention compare recipes rather than profile IDs, so a reservation change
does not change any job's identity.

No hash covers system binaries such as those under `/usr/bin` and `/bin`,
undeclared files next to declared tools, package-manager code managed by tools
such as corepack, container images pulled by tag, host caches, the kernel, the
network or which host executed the job.

Commands run in private attempt directories with a cleared environment and
private `HOME` and `TMPDIR`. `PATH` contains declared tool directories and system
directories. Add required non-secret environment values explicitly. Shell
expansion only happens when a command explicitly invokes a declared shell.
Declare adapter scripts and sidecar configuration as tool entries when they are
outside the source revision.

## Prepare once, consume independently

Dependency downloads, generated code, compiled libraries and test fixtures can
be preparation jobs. A downstream job lists their names in `dependencies`.
Patchweft schedules dependencies first and copies their declared artifacts into
each consumer's working directory after checking their hashes.

```json
{
  "version": 1,
  "name": "shared-preparation",
  "certifying": false,
  "tools": {"python": "/usr/bin/python3"},
  "jobs": [
    {
      "name": "prepare",
      "command": ["python", "prepare.py"],
      "artifacts": ["prepared"],
      "report": {"kind": "command"}
    },
    {
      "name": "check-a",
      "dependencies": ["prepare"],
      "command": ["python", "check.py", "a"],
      "report": {"kind": "command"}
    },
    {
      "name": "check-b",
      "dependencies": ["prepare"],
      "command": ["python", "check.py", "b"],
      "report": {"kind": "command"}
    }
  ]
}
```

The [runnable example](../examples/shared-prep/README.md) supplies these scripts,
resolves the local Python path and verifies one preparation plus two consumers.
One consumer changes its copy; the other still sees the original artifact.

Artifacts cannot overwrite source files. Conflicting outputs from dependencies,
symlinks and escaping paths are rejected. Arbitrary installed package directories
often contain symlinks or machine-specific paths. Prefer regular archive files or
downloaded package files, then unpack or install into each consumer's private
workspace. Do not share a mutable installation directory between running jobs.

For Python dependencies, a preparation script can run
`python -m pip download --only-binary=:all: --require-hashes -r requirements.lock -d wheelhouse`.
Consumers can install from that artifact using
`python -m pip install --no-index --find-links wheelhouse --only-binary=:all: --require-hashes -r requirements.lock`
inside a private virtual environment. Supply a complete hash-pinned lockfile,
compatible platform packages and a pinned Python/pip environment. Those package
tools are project prerequisites; Patchweft does not install them automatically.
The wheel-only form avoids an implicit source-build dependency download. See
[pip download](https://pip.pypa.io/en/stable/cli/pip_download/) and
[pip's hash-checking guidance](https://pip.pypa.io/en/stable/topics/secure-installs/)
for the package-manager requirements.

Network fetching should default to fresh execution or in-flight sharing.
Completed reuse needs an audit of package identity, platform, installer, scripts
and external inputs. A URL or lockfile alone does not close every input.
An author workspace is initially a source checkout; preparation is not an automatic
editor setup hook. Local agents can explicitly `deliver ACTION_ID --destination
NEW_DIRECTORY` and use the verified artifact.

## Choose a reuse policy

| Policy | Behavior |
|---|---|
| `inflight` (default) | Outstanding work is shared with any profile of the same evidence class whose job identity and timeout match. A new direct request after completion starts a fresh observation. |
| `never` | Each request creates an independent action. |
| `exact` | Completed results may be reused for the same complete source commit, job identity, timeout and dependency actions. Requires an audit. |
| `files` | Materializes only explicitly listed regular source files; supports completed reuse across revisions with identical inputs. Requires an audit and no job dependencies. |
| `declared` | Keyed on the files the revision's declarations file says the job reads; reuses an earlier pass with the same declared inputs across revisions. Requires an audit and the profile's `inputs_file`. An undeclared job behaves as `inflight`. See [declared inputs](#declared-inputs). |

Use `inputs` only with `files`, and include every source input, such as both the
lockfile and preparation script. A files-only job has no implicit full checkout.
Sharing and reuse are scoped to the project, job identity, evidence class and
timeout. An existing audit therefore also covers other profiles with an
identical job identity; audit wording is not part of identity.

Registering a new profile no longer forces fresh runs: a job whose identity is
unchanged reuses across profiles under `exact`, `files` or `declared`. Identity covers the
recipe's declared tools, environment and command, not the host. After a host
change the recipe cannot see, such as a system compiler upgrade, a corepack
update or an image pulled by a moving tag, force a fresh observation
deliberately: change something in the recipe (for example an environment
variable naming the host change) or set the job's `reuse` to `never`.

The `audit` field records why completed reuse is sound. The engine checks that
an audit exists, not whether its reasoning is true. Native execution still sees
host libraries and can use the network. Hashing a compiler launcher does not hash
the compiler and all its libraries. Reusing the same result is not a fresh test.

## Declared inputs

A `declared` job is keyed on the files the project says it reads instead of the
whole commit. The profile's `inputs_file` names the declarations file, a path in
the source such as `scripts/ci/patchweft-inputs.json`. Patchweft reads it at
submit from the revision under test:

```json
{"schema": "patchweft.inputs/1",
 "common": ["package.json", "**/package.json", "pnpm-lock.yaml"],
 "jobs": {
  "lint": {"paths": ["**"], "exclude": ["docs/**", "**/*.md"], "why": "measured reads …"},
  "vitest-*": {"paths": ["**"], "exclude": ["docs/**"], "keep": ["docs/x.md"], "why": "…"},
  "test-proofs": {"paths": ["scripts/**"], "base_paths": ["packages/db/migrations/**"], "why": "…"},
  "census": {"whole_tree": "lists the whole tree"}}}
```

- A job's inputs are `common`, plus its `paths` less `exclude`, plus `keep`,
  matched against the revision's tracked files, plus every tracked
  `.gitattributes` on an input's ancestor path (checkout applies their line
  endings and filters). `exclude` never removes a `common` file. The job's own
  entry and `common` are part of its identity; the rest of the declarations file
  is not, so rewording a `why` or another job's entry reruns nothing.
- `base_paths` are matched against the base: the commit a `Patchweft-Base:`
  trailer in the commit message names. With no trailer, or one that names no
  commit, there is no base and a job with `base_paths` keeps the commit key. The
  job receives the base it was keyed at as `PATCHWEFT_BASE`.
- A key names a job exactly or a family `prefix*`. An exact name wins, then the
  longest family. A key that names no job of the profile is ignored and recorded
  as an `inputs.unmatched` event.
- Patterns are anchored at the repository root and case-sensitive. `*` matches
  within one path segment, leading dots included; `?` matches one character;
  `**`, only as a whole segment, matches any number of segments. Braces,
  brackets, backslashes, a leading `!` or `/` and empty segments are refused.
  [`inputs-dialect-vectors.json`](inputs-dialect-vectors.json) publishes the dialect as test vectors:
  any other implementation, such as an adapter that checks a reused archive, must agree with each one.

The declared identity covers the job's entry without `why`, `common`, and the
path, mode and object ID of every input and base file. A declared job reuses an
earlier `passed` action with the same recipe, evidence class, timeout and
declared identity, and whose dependencies' passed keys are the same, from any
revision of the project. A dependency's output is an input of its dependents, so
each job's passed key folds in every dependency's passed key, and a job with a
dependency that has none (commit-keyed or on the fail-safe path) has none itself.
Its dependency actions are not compared: a dependency rerun on equal inputs keeps
its passed key. Only a `passed` action is reused this way. Queued and running work is still shared under
the commit and dependency actions exactly as under `inflight`, so outstanding
work never joins across revisions. A failed, interrupted, canceled or stopping
action is never reused.

A reused job is listed in the candidate's `reused` with the action and the
candidate and revision it ran for, and emits `action.reused`. `status` shows the
job as `passed` with its `reused` entry, and the frontier certificate marks the
receipt `reused`. A declared job with a passed key receives it as
`PATCHWEFT_INPUTS`: the same inputs and dependencies give the same value, so a
job can derive its own run identities (a seed) from it. The value differs from
job to job, because the recipe names the job, so an identity several jobs share
(a run id across shards) must come from something train-wide instead. A declared
job with `base_paths` receives the base it was keyed at as `PATCHWEFT_BASE`,
whether or not it has a passed key, since it needs its base either way.

Before registering a profile with `declared` jobs, every executor must run a
binary that knows the policy, and the jobs themselves must not pass vacuously: a
job that skips its checks when "nothing it covers changed" (an adapter's own
applicability test) exits 0, and Patchweft cannot tell that skip from a pass, so
the declared key must replace such a skip rather than sit beside it.

Each of these keeps a job on today's `inflight` behaviour, keyed on the commit
and its dependency actions with no completed reuse:

- its entry is absent or `whole_tree` (the value is the reason);
- it has `base_paths` but no base;
- one of its dependencies has no passed key (commit-keyed, or itself on this list);
- a symlink or submodule is among its matched files;
- the declarations file is missing or invalid. That applies to every declared
  job, is recorded as an `inputs.invalid` event with the reason, and never fails
  the submit.

The file is invalid when it has another schema, an unknown field or a duplicate
key at any level, when a
pattern is outside the dialect, when a `common`, `paths`, `keep` or `base_paths`
pattern matches no tracked file (`base_paths` at the base), when an entry has
neither or both of `paths` and `whole_tree`, or when a `why` is missing or under
20 characters.

The declarations are the project's claim, like `audit`: Patchweft does not check
that a job reads only what it declares. A reused pass is the earlier
observation, not a fresh test. As for `exact` and `files`, a host change the
recipe cannot see, such as a new compiler or Docker release, needs a recipe
change to force a fresh run.

## Retain passed obligations after a profile change

A new profile that only changes reservations, or changes some jobs, would
otherwise rerun every obligation. Submit the same revision under the new profile
with `--retain-from PREVIOUS_CANDIDATE_ID` (`retain_from` over the API) to keep
the predecessor's passes whose identity still holds. A job keeps the
predecessor's action only when all of these hold:

- both candidates name the same revision; other revisions are refused. Over the
  API, the caller also needs read access to the predecessor's project and a grant
  for the predecessor's profile;
- the job's reuse policy is not `never`;
- the predecessor's action passed, recorded a job identity, and its evidence
  class matches. Certifying and exploratory evidence never cross;
- the job's recipe is unchanged, and the action consumed the same dependency
  actions that this submission resolved. A rerun upstream reruns its consumers;
- the observed time, which includes source preparation, is below the new timeout.

Every other job takes the normal request path, with the new profile's
reservation and the new candidate's own upstream actions. The candidate records
`retained_from`, the `retained` jobs and a reason for each job in
`not_retained`; each retention emits an `action.retained` event, and the frontier
certificate marks those receipts as retained. A retained pass is the earlier
observation, not a fresh test. Without `--retain-from`, a submission under the
new profile takes no completed pass, except through the `exact`, `files` and
`declared` policies above.

Job identity does not see host toolchain changes, such as a new system compiler,
container image or package-manager release. Do not retain across those changes.

## Validate useful evidence

Every job needs a `report`:

- `command`: a successful process exit is the evidence contract.
- `node-tap`: parse Node test output with a positive `minimum` executed count.
- `vitest`: parse a fresh JSON report at `path`, with a positive `minimum`.
- `junit`: parse a fresh XML report at `path`, with a positive `minimum`.

Test reports reject skips unless `allow_skips` is explicitly true. File reports
are removed before execution and must be freshly produced inside the attempt
workspace. Counts do not establish an exact test roster or sufficient coverage;
those remain part of the project's policy.

Each job can specify `slots`, `memory_mb` and `timeout_seconds`. These default
to 1 slot, 1024 MiB reserved and 1800 seconds. Reservations are admission units,
not hard CPU/RSS limits. Each artifact file and each log may hold at most 1 GiB,
and an attempt's artifacts and logs together at most 4 GiB; exceeding either
fails the attempt on every host. Declare a bounded `cleanup` argv for external resources
such as project-owned containers. Uncertain cleanup retains capacity for an operator.

`memory_mb`, and `slots` when `export_slots` is false, form a per-action
reservation. A queued action shared by several profiles reserves the largest
value each one requested, and retry raises it to the retrying candidate's
profile. A running action is joined only when its attempt reserved at least as
much; otherwise the requester gets a new action. Reservations never decrease;
cancel and resubmit to lower one. The timeout is part of an action's identity,
so profiles that differ only in timeout do not share outstanding work.

By default `slots` is exported to the job as `PATCHWEFT_SLOTS` and is part of
its identity. Set `"export_slots": false` when the job does not read that
variable; slots then only control admission. Check the job's scripts and tools
for `PATCHWEFT_SLOTS` before setting it.

The timeout budget starts before source and dependency preparation. The
supervisor checks it during command supervision; preparation is not independently
interrupted by this timer. A remote executor's downloads before launch are not
charged to it. Allow for preparation when choosing the budget.
Cleanup runs afterward, so the job timeout is not a hard bound on the complete
attempt lifecycle.

Jobs receive `PATCHWEFT_STATE`, `PATCHWEFT_ATTEMPT`, `PATCHWEFT_REVISION`,
`PATCHWEFT_SLOTS` (unless `export_slots` is false), `PATCHWEFT_REPORT`,
`PATCHWEFT_VACUOUS` and, for
declared jobs with a passed key, `PATCHWEFT_INPUTS` and, for declared jobs with
`base_paths` and a base, `PATCHWEFT_BASE` (passed key or not).
`PATCHWEFT_VACUOUS` is an absolute path in the attempt directory, outside the
checkout, that does not exist at launch: a job that passes without doing its work
(its surface untouched) may create it, and the pass then records `"vacuous": true`
in its evidence and is not counted toward the job's expected duration, while its
state, reuse, events and status are those of any other pass.
`PATCHWEFT_STATE` is the store's directory on the API host and the executor's
`--work` directory elsewhere; `$PATCHWEFT_STATE/attempts/$PATCHWEFT_ATTEMPT` is the
attempt directory on both.
Cleanup receives the state and attempt identifiers plus `PATCHWEFT_WORKSPACE`,
the absolute job checkout path. Cleanup
starts from the protected attempt directory so a job changing its checkout's
permissions cannot prevent cleanup from launching. Use the explicit workspace
variable when cleanup needs job files. These are trusted execution hooks, not a
permission boundary.

Attempt evidence records source preparation time, dependency verification and
copy time, actual dependency bytes copied, command time and artifact-capture
time. It also records the executing profile, the job identity and action key,
the applied budget, the hostname, the `executor`, and `timer_seconds`: the
timeout clock when supervision ended, after detached descendants exited.
`preparation.transfer_seconds` and `transfer_bytes` record what the executor
downloaded for the attempt before launch; they are 0 on the API host and when
the source and dependencies were already on the executing host. The executor and
host are observations, not part of job identity. Copy time excludes archive
extraction performed by job scripts. These
measurements describe observed work; a shared request alone does not establish
CPU or elapsed time saved.

## External resource hooks

An adapter that starts containers, databases or other external resources owns
their lifecycle. Patchweft supervises native descendants and calls the declared
cleanup command; it cannot discover or verify those resources on the adapter's
behalf. Make the cleanup contract explicit:

- Assign an attempt-specific identity before creation. Persist actual resource
  identifiers in a durable record outside the job checkout. If creation succeeds
  before that record is written, cleanup must discover only resources bearing
  that attempt's identity and persist the recovered identifiers before removal.
- Record dependent resources, such as volumes, before deleting the parent that
  exposes their identifiers. Keep those records through interrupted cleanup so
  a later invocation can finish even when the parent is already absent.
- Verify ownership before removal and verify absence afterward. Treat missing
  owned resources as successful cleanup, while distinguishing absence from an
  unavailable service or an inspection error. Never use global prune operations.
- Make cleanup safe to repeat after partial creation, command failure, timeout,
  cancellation or interruption. Return success only when the resources are
  confirmed drained; an uncertain result must retain the reservation.

Exercise the ordinary workload path when checking an execution boundary. A
startup refusal proves that invocation was rejected, not that its payload's
assertions ran. Keep the exact command, exit status and partial output for each
probe. If a changed invocation is needed to execute the payload, record that
change and limit claims to what it actually tested. Register a new profile when
tools or configuration change; resubmit with `--retain-from` to keep passes
whose job identity is unchanged. Changing any declared tool's bytes changes
every job's identity, because every tool directory is on every job's `PATH`.
