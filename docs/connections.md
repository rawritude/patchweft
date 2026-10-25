# Patchweft connections

Patchweft serves HTTP/JSON commands and queries, a resumable event stream, and a thin
MCP adapter. All three use the same authenticated application handlers and durable
mutation receipts. Local administrative commands still open the state store directly.
The server and the local `worker` must use the same binary, state directory and host;
remote executors use the same binary on other Linux hosts through the
[executor routes](#executor-routes). Remote clients (`remote` and `mcp`) also build on
macOS; supervised job execution requires Linux and refuses to start elsewhere.

This is a single-store service for cooperating agents. Its scoped network API does
not isolate executed code: trusted registered profiles still run with the executing
account's Unix privileges. Automatic store retention, replication, OAuth
provisioning and a hostile-code sandbox are not implemented.

## Start a connection

An operator imports projects and registers profiles with the existing local CLI.
Then issue a credential using a policy containing the returned project/profile IDs:

```json
{
  "id": "agent-a",
  "projects": ["PROJECT_ID"],
  "profiles": ["PROFILE_ID"],
  "roles": ["read", "write"],
  "max_pending": 1024
}
```

```bash
patchweft --state /private/patchweft grant --policy agent-a.policy.json \
  --credential-file /private/agent-a.credential
patchweft --state /private/patchweft serve --listen 127.0.0.1:7412
patchweft --state /private/patchweft worker
patchweft remote --endpoint http://127.0.0.1:7412 \
  --credential-file /private/agent-a.credential query --data '{"op":"projects"}'
```

`grant` creates a new owner-private credential file without printing its contents.
The store retains a digest. Reissuing a grant rotates that principal's credential and
replaces its policy. `patchweft --state /private/patchweft policy --policy FILE`
replaces an existing principal's scope (for example after registering a new profile)
and leaves its issued credentials working; a policy without `max_pending` keeps the
current limit. `patchweft --state /private/patchweft revoke PRINCIPAL` revokes its
credentials. Each of the three records an event (`principal.granted`,
`principal.policy` with the policy before and after, `principal.revoked`). Grant,
policy and revoke, project import, profile registration and confirmed-drain recovery
are local operator operations.

Non-loopback listeners require `--tls-cert CERT --tls-key KEY`. Remote clients require
HTTPS; HTTP is permitted only for loopback. Use `--ca-cert CERT` for a private CA.
The client rejects redirects and credential-bearing endpoint URLs. The server checks
any supplied `Origin` against explicit `--allow-origin ORIGIN` values, with none
allowed by default. This is not a browser CORS integration.

Principals have project and profile allowlists. `read` permits queries and downloads;
`write` permits source publication/composition, requests, submissions, cancellation
and retry. `review` permits review and repair-resolution attestations; `promote`
permits frontier promotion. `operator` permits metrics queries. `execute` makes the
principal the executor of the same name (see [executor routes](#executor-routes)); it
requires `read`, and `local` cannot hold it. A role never bypasses project scope. Candidate prerequisites are checked recursively, including cross-project
prerequisites. Mutation replays and operation lookups recheck current permissions.
The server derives the owner as `api:PRINCIPAL`; caller-supplied owner fields are refused.

## Commands and receipts

POST `/v1/commands` with a JSON command, `Authorization: Bearer …`, and an
`Idempotency-Key` of 1–128 visible ASCII bytes. Use a new key for each logical mutation;
preserve that key and exact command when retrying a lost response or transient failure.
The CLI keeps credentials out of argv:

```bash
patchweft remote --endpoint http://127.0.0.1:7412 \
  --credential-file /private/agent-a.credential command --key change-17-validation \
  --data '{"op":"submit","revision":"REVISION_ID","profile":"PROFILE_ID"}'
```

A successful response contains `api_version`, `operation_id`, `status`, `replayed`
and `result`. The operation ID names the durable receipt; action/candidate IDs in
`result` name ongoing work. Receipt creation and database effects share one SQLite
transaction. Concurrent retries of a key return the same receipt. Reusing a key with
a different command returns HTTP 409. No HTTP connection must stay open while a job runs.

| `op` | Fields | Result / authority |
|---|---|---|
| `request` | `revision`, `profile`, `job` | Action; write |
| `submit` | `revision`, `profile`, optional `requires` candidate IDs, optional `retain_from` candidate of the same revision | Candidate; write, plus a grant for the `retain_from` candidate's profile |
| `cancel` | `action` | Detaches only caller's interests; write |
| `retry` | `candidate`, optional `jobs` | Retries unsuccessful obligations with confirmed drain; write plus caller association |
| `compose` | `base`, `changes`, `message` | Revision, or durable conflict repair handle; write |
| `publish` | `project`, uploaded `sha256`, Git `commit`, `message` | Published revision; write |
| `review` | `revision` | Exact-revision review; review |
| `resolve` | `repair`, passing `candidate` from the same project | Repair attestation; review |
| `promote` | `candidate`, optional `expected` previous candidate | Frontier certificate/CAS; promote |

A composition conflict returns HTTP 200 with `result.state="conflict"` and `repair`;
this means the conflict was recorded, not that composition passed. Inspect the repair.
Other failed command preconditions produce a durable 409 receipt with
`error.code="command_rejected"`; changing circumstances requires a new key. Validation,
authorization, queue-budget and internal transaction failures do not create a receipt.
Their responses carry `api_version` and structured `error.code`/`message`.

`retain_from` keeps passing obligations of an earlier candidate for the same revision
whose job identity is unchanged; see
[retention](profiles.md#retain-passed-obligations-after-a-profile-change). A submit with
`retain_from` is a different command from one without it, so derive the idempotency key
from `retain_from` too; reusing a key from a plain submit returns HTTP 409.

Retry preserves passing obligations. For shared candidates the requesting principal
must have a recorded interest in that candidate; the API reactivates only that caller's
interests. Canceling a transport request or MCP RPC does not cancel accepted work.

Git import/composition may leave unreachable objects or internal refs if the database
transaction fails. Only a committed revision record makes that source available through
the API. The transaction guarantee covers the database effect and receipt; it does not
claim a distributed transaction with Git or external job side effects.

## Queries and events

POST `/v1/query` with a typed JSON query. Responses contain `api_version` and `result`.
`patchweft remote … query --data JSON` returns that result directly.

| `op` | Fields |
|---|---|
| `projects` | optional `after`, `limit` |
| `profiles` | none; returns permitted IDs, names, resource requirements, timeouts, job dependencies and job identities (`recipe`) |
| `revisions`, `actions`, `repairs` | `project`, optional `after`, `limit` |
| `action` / `candidate` / `repair` | corresponding ID field; an action includes its key, job identity, reservation and origin |
| `frontier` | `project`; returns the validated candidate and its certificate |
| `logs` | `attempt`, optional byte `offset`, byte `limit` |
| `artifacts` | `action`; returns the passing attempt's file manifest |
| `operation` | receipt `id`; caller-owned and currently authorized |
| `events` | optional `after`, `project`, `limit` |
| `metrics` | none; operator role |

List pages default to 100 rows and accept 1–1000. Persist `next_after` and continue
while `more` is true. Logs return base64 bytes with `next_offset` and observed size;
a read is limited to 64 KiB. No query exposes a server filesystem path as a client
workspace instruction.

GET `/v1/events` returns a JSON event page, or an SSE response when `Accept` includes
`text/event-stream`. Use `Last-Event-ID` or `after`, not conflicting values. SSE
`patchweft` messages carry durable event sequence IDs; `cursor` messages advance over
rows hidden by project scope. Persist the latest delivered cursor, including cursor
messages. A cursor ahead of the store is rejected. No event retention/deletion policy
exists yet, so old cursors remain replayable while the store is retained.

```bash
patchweft remote --endpoint http://127.0.0.1:7412 \
  --credential-file /private/agent-a.credential events --after 0 --follow
```

The CLI emits JSON lines, flushes each complete event, and reconnects from its last
flushed cursor on EOF, reset, timeout or transient server overload. Backoff grows to
five seconds. Authorization and invalid-cursor failures stop the follower. A new CLI
process needs the cursor saved by its caller; stdout is not a transactional consumer
checkpoint. Consumers should deduplicate effects by event sequence.

## Source and artifact transfer

Remote agents author in their own Git repository. Create a stable bundle file and
publish it with an explicit commit and idempotency key:

```bash
git bundle create /work/change.bundle HEAD
patchweft remote --endpoint http://127.0.0.1:7412 \
  --credential-file /private/agent-a.credential publish \
  --project PROJECT_ID --bundle /work/change.bundle --commit COMMIT_SHA \
  --message "Publish change" --key change-17-source
patchweft remote --endpoint http://127.0.0.1:7412 \
  --credential-file /private/agent-a.credential source REVISION_ID \
  --destination /work/received.bundle
patchweft remote --endpoint http://127.0.0.1:7412 \
  --credential-file /private/agent-a.credential deliver ACTION_ID \
  --destination /work/new-artifacts
```

The upload route is PUT `/v1/blobs/SHA256` (role `write` or `execute`); bytes must
match that digest. Publication
verifies the Git bundle and advertised commit before importing into the permitted
project. A source download is GET `/v1/revisions/REVISION_ID/bundle`; its response
includes a SHA-256 header checked by the CLI. Artifact downloads use
GET `/v1/artifacts/ACTION_ID/SHA256`, with membership in an authorized passing manifest
and content verified before streaming. The CLI also verifies content, safe relative
paths and recorded modes, then atomically publishes a new destination directory.
For an authorized action that has not passed, both the manifest query and download
route return HTTP 409 with code `artifacts_unavailable`. Inspect its status and logs;
failed work does not produce a deliverable artifact manifest. This replaces the
previous generic HTTP 500 response. Unknown or out-of-scope actions remain forbidden.
Existing destinations are never overwritten. Reuse the same bundle file on publication
retry: independently regenerated bundle bytes need not have the same SHA-256.

The source-download route exports full reachable history, with a **1 GiB** limit
for both uploads and downloads. `?have=SHA,SHA,…` (at most 32 full lowercase commit
IDs, else HTTP 400 `invalid`) leaves out history those commits reach. Haves that are
not commits of the project are ignored; when they already reach the revision, the
full bundle is sent. An incremental bundle verifies only in a repository holding its
prerequisites. Uploads stream through a bounded disk writer; downloads stream from
files. A transfer does not buffer the whole bundle in memory. After the server has
the prerequisite base, subsequent uploads can use a bundle such as
`git bundle create /work/change.bundle BASE_COMMIT..HEAD`.
Unconsumed uploads are deleted after 24 hours.

## Executor routes

A remote executor's coordinator (`patchweft executor`) uses four POST routes with a
JSON body of at most 2 MiB and responses in the `{"api_version":"1","result":…}`
envelope. The executor ID is the principal ID; it needs role `execute` and a record
created with `executor-set`. Its current project and profile grants are rechecked on
every call.

| Route | Body → result | Admission |
|---|---|---|
| `/v1/executor/profiles` | `{"ids":[…]}` (≤ 32) → executor ID, lease and permitted profiles; no IDs: those with queued work | control |
| `/v1/executor/claim` | claim key, `verified` and `refused` profiles, observations → a claim, `fenced`, or profiles to verify | work |
| `/v1/executor/renew` | ≤ 64 `{attempt, lease, started?, stopping?}` → per lease `ok` or `fenced`, `stop`, `registered` | control |
| `/v1/executor/complete` | attempt, lease, outcome, evidence, uploaded `log`/`cleanup_log`/`artifacts` → `recorded` or `fenced` | work, 600 s deadline |

A claim key (1–128 visible ASCII bytes, else 400 `invalid_key`) returns the attempt it
already claimed, so a lost response never claims twice. A coordinator that never
recorded the attempt a key was `fenced` for sends the key again with
`"unrecorded":ATTEMPT_ID`: a lapsed attempt that never started is then drained and, within
the self-heal bound (see [operations](operations.md#executors)), its action queued again or
canceled if nobody requests it; anything else is left as it is. Such a claim never claims
new work. A `refused` entry `"*"`
declines every profile. A completion first uploads each blob it names; if one is
absent the route answers 422 `missing_blob` with `error.detail.missing` listing the
digests to upload again. An identical completion is answered again as a replay; a
different one for a completed attempt is 409 `completion_conflict`. A report for an
attempt this executor no longer holds under that lease is 409 `attempt_fenced` and
changes nothing, except that the lease holder of an expired attempt may report its
drain, and that holder's undrained report is recorded as `refused` on the attempt before
the 409. Other errors: 403 `forbidden` (no `execute` role, or `verified` outside the
profile grant), 403 `executor_unregistered`, 422 `invalid_evidence`.

## MCP

`patchweft mcp --endpoint ORIGIN --credential-file FILE [--ca-cert CERT]` runs a newline JSON
stdio adapter. The HTTP server also accepts POST `/mcp`. Both expose `patchweft_query` and
`patchweft_command` with typed input schemas; `patchweft_command` takes `idempotency_key` and `command`.
Use the same command key when retrying across stdio, HTTP MCP and the ordinary API.

For a client that accepts an `mcpServers` configuration, a stdio entry looks like:

```json
{
  "mcpServers": {
    "patchweft": {
      "command": "/absolute/path/to/patchweft",
      "args": [
        "mcp",
        "--endpoint", "https://your-host.example:7412",
        "--credential-file", "/private/agent-a.credential"
      ]
    }
  }
}
```

Replace the binary, endpoint and credential-file paths. Configuration shape is
client-specific; this example is not a claim of tested support for every client.
Credential contents stay in the private file. The operator must start the API
and worker separately.

The adapter implements the 2026-07-28 request metadata/header rules and a 2025-11-25
compatibility path. Modern clients use `server/discover` and per-request protocol
metadata; legacy stdio clients initialize and send `notifications/initialized`.
Modern HTTP requires matching `MCP-Protocol-Version`, `Mcp-Method` and, for a tool call,
`Mcp-Name`, plus JSON/SSE Accept types. Tool-domain failures appear in `isError` and
structured content even when the JSON-RPC HTTP response is 200. No-ID notifications
never invoke application mutations.

This is a stateless tool service. The durable Patchweft event stream lives at
`/v1/events`; it is separate from MCP transport streams. Authentication uses operator
provisioned scoped Bearer credentials. Standard MCP OAuth discovery/registration and
compatibility with every vendor's MCP client have not been established.

## Bounds and measurement

The server admits up to 64 authenticated HTTP calls, with eight per principal.
Different credentials for one principal share that budget. Authentication has
16 separate execution slots, with two per credential. Ordinary application work
has 24 slots, with two per principal; queries (including event polls) and explicit
action cancellation have eight separate slots, with four per principal. These
limits apply to both HTTP and MCP. Excess work receives `503 busy` without joining
an unbounded queue. Admission records are reclaimed when unused, with at most
256 credential records and 256 principal records held at once.

Event streams retain their existing 32-slot budget and release HTTP call admission
after sending headers; subsequent polls still use the control execution budget.
This is a limit on open streams, not simultaneous polls from one principal. If a
poll is refused after headers, the stream emits an error event and closes instead
of changing its HTTP status.
Transfers are limited to four. Ordinary requests have a 35-second server
deadline; receiving an upload or preparing a source download has a 600-second deadline.
The remote CLI allows 600 seconds for each transfer, including response-body download.
The server does not impose that deadline on a download body after sending its headers.
Application blocking closures retain their admission and execution permits after
a caller disconnects or a deadline expires.
An upload's disk writer owns its temporary file and transfer permit until writing and
finalization finish, even if the client cancels; incomplete streams are discarded.
Download permits remain held through the response stream. Upload ingress is also
limited to four. Regular JSON bodies are limited to 2 MiB; source
bundles to 1 GiB and retained uploads to 4 GiB per principal. Pending action budgets
are checked after coalescing, within the same rollback boundary: configurable per
principal, with a 10,000-action project ceiling.

Backpressure returns 429 or 503 where a response can be delivered. Network clients
must also handle transport errors with a stable mutation key. The native profile's
slot and memory values are admission reservations, not OS resource enforcement.
Artifact and log files are limited to 1 GiB each and an attempt's outputs to 4 GiB.
History and store disk use are not governed by automatic quotas.

Metrics count commands, queries, receipt replays and failed transactions, plus cumulative
microseconds waiting for the command gate/SQLite transaction, executing commands and
committing. These are process-local counters and reset when the API restarts. Worker
processes have separate execution state; they are not included in API timing counters.
Project permissions do not provide per-project latency isolation under overload.
The control execution reserve protects against admitted ordinary work consuming
all blocking capacity; it is not a latency guarantee. Slow request bodies can
occupy a principal's shared HTTP budget, authentication has its own finite budget,
and cancellation still waits for an in-progress database writer. Multiple busy
principals, event polls, storage and host load can also exhaust shared resources.
