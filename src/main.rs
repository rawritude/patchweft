use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand};
use patchweft::{
    api, authority, client, engine, executor, http, mcp, model, source, store::Store, worker,
};
use serde_json::{Value, json};
use std::{
    fs,
    net::SocketAddr,
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};

const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Parser)]
#[command(
    name = "patchweft",
    version,
    about = "Shared revisions and computation for cooperating agents"
)]
struct Cli {
    #[arg(long, global = true, default_value = ".state")]
    state: PathBuf,
    #[command(subcommand)]
    command: Operation,
}

#[derive(Subcommand)]
enum Operation {
    /// Serve authenticated HTTP/JSON, resumable events and the MCP endpoint.
    Serve {
        #[arg(long, default_value = "127.0.0.1:7412")]
        listen: SocketAddr,
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        #[arg(long)]
        tls_key: Option<PathBuf>,
        #[arg(long)]
        ready_file: Option<PathBuf>,
        #[arg(long)]
        allow_origin: Vec<String>,
    },
    /// Issue or rotate a scoped credential into a new private file; bytes are never printed.
    Grant {
        #[arg(long)]
        policy: PathBuf,
        #[arg(long)]
        credential_file: PathBuf,
    },
    /// Set an owner's scheduling priority (higher is admitted first; 0 clears it).
    Priority {
        owner: String,
        #[arg(allow_negative_numbers = true)]
        priority: i64,
    },
    /// Keep part of an executor's capacity for owners above priority 0 (0 and 0 clear it).
    Reserve {
        executor: String,
        #[arg(long)]
        slots: u32,
        #[arg(long)]
        memory_mb: u32,
    },
    /// Replace an existing principal's scope without rotating its credentials.
    Policy {
        #[arg(long)]
        policy: PathBuf,
    },
    /// Revoke all credentials for a principal.
    Revoke { principal: String },
    /// Use the remote API without exposing credential bytes in argv.
    Remote {
        #[command(flatten)]
        connection: Connection,
        #[command(subcommand)]
        operation: RemoteOperation,
    },
    /// Run a stdio MCP adapter connected to an Patchweft API.
    Mcp {
        #[command(flatten)]
        connection: Connection,
    },
    /// Create a durable local service store with a shared execution budget.
    Init {
        #[arg(long, default_value_t = 8)]
        slots: u32,
        #[arg(long, default_value_t = 16000)]
        memory_mb: u32,
    },
    /// Import a committed snapshot from a local repository.
    Import {
        #[arg(long)]
        name: String,
        #[arg(long)]
        repo: PathBuf,
        #[arg(long, default_value = "HEAD")]
        rev: String,
        #[arg(long)]
        owner: String,
    },
    /// Create an isolated author directory from an available revision.
    Workspace {
        #[arg(long)]
        revision: String,
        #[arg(long)]
        owner: String,
    },
    /// Publish working edits as an immutable revision without changing author files or staging.
    Checkpoint {
        workspace: String,
        #[arg(long)]
        message: String,
    },
    /// Combine explicit revisions; conflicts become repair records.
    Compose {
        #[arg(long)]
        base: String,
        #[arg(long, required = true)]
        change: Vec<String>,
        #[arg(long)]
        owner: String,
        #[arg(long, default_value = "Compose changes")]
        message: String,
    },
    /// Register a versioned, trusted execution policy and its tools.
    Register { profile: PathBuf },
    /// Request one computation, coalescing identical outstanding requests.
    Request {
        #[arg(long)]
        revision: String,
        #[arg(long)]
        profile: String,
        #[arg(long)]
        job: String,
        #[arg(long)]
        owner: String,
    },
    /// Request every obligation for one exact composition, independently of other candidates.
    Submit {
        #[arg(long)]
        revision: String,
        #[arg(long)]
        profile: String,
        #[arg(long)]
        owner: String,
        #[arg(long)]
        requires: Vec<String>,
        /// Retain passed obligations of a same-revision candidate whose job identity is
        /// unchanged; retained passes are earlier observations, not fresh tests.
        #[arg(long)]
        retain_from: Option<String>,
    },
    /// Run the local executor's coordinator; --drain exits when all currently runnable work is
    /// finished. A second coordinator waits as a standby.
    Worker {
        #[arg(long)]
        drain: bool,
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..))]
        poll_ms: u64,
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..))]
        heartbeat_seconds: u64,
        /// Claims wait while less is free under the state directory.
        #[arg(long, default_value_t = 0)]
        min_free_gb: u64,
        /// Loopback ports jobs may reach; any other loopback listener refuses all work. Checked by
        /// default (MUST-FIX 8, follow-up 1), so it cannot regress silently.
        #[arg(long, value_delimiter = ',')]
        loopback_allow: Vec<u16>,
        /// Skip the loopback check: only for a host whose every loopback service may be reached by
        /// jobs.
        #[arg(long)]
        no_loopback_check: bool,
    },
    /// Run a remote executor's coordinator against an API; --drain exits when all currently
    /// runnable work is finished. A second coordinator for the same work directory waits as a
    /// standby.
    Executor {
        #[command(flatten)]
        connection: Connection,
        /// `PATCHWEFT_STATE` for jobs: attempts, source mirrors, the artifact cache.
        #[arg(long)]
        work: PathBuf,
        #[arg(long)]
        drain: bool,
        #[arg(long, default_value_t = 2000, value_parser = clap::value_parser!(u64).range(1..))]
        poll_ms: u64,
        #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
        heartbeat_seconds: u64,
        /// Claims wait while less is free under the work directory, after reclaiming space.
        #[arg(long, default_value_t = 20)]
        min_free_gb: u64,
        #[arg(long, default_value_t = 20)]
        cas_max_gb: u64,
        /// Fenced or undrained attempt directories are kept this long.
        #[arg(long, default_value_t = 72)]
        retain_hours: u64,
        /// Loopback ports jobs may reach; any other loopback listener refuses all work. Jobs run
        /// natively, so this holds whatever the Docker daemon exposes (follow-up 1).
        #[arg(long, value_delimiter = ',')]
        loopback_allow: Vec<u16>,
        /// Skip the loopback check: only for a host whose every loopback service may be reached by
        /// jobs.
        #[arg(long)]
        no_loopback_check: bool,
    },
    /// Register or reconfigure an executor. `local` sets the store's own budget; a remote
    /// executor runs only the jobs placed on it with --allow-job.
    ExecutorSet {
        id: String,
        #[arg(long)]
        slots: Option<u32>,
        #[arg(long)]
        memory_mb: Option<u32>,
        #[arg(long)]
        lease_seconds: Option<u64>,
        /// Replaces the jobs this executor may run.
        #[arg(long)]
        allow_job: Vec<String>,
        /// Replaces the jobs this executor never runs; a denial wins over an allowance.
        #[arg(long)]
        deny_job: Vec<String>,
        /// Reset placement before applying --allow-job and --deny-job.
        #[arg(long)]
        clear_placement: bool,
    },
    /// Show executors: budgets, held attempts, placement and their coordinators' reports.
    Executors,
    /// Inspect exact source identities, action states and unmet prerequisites.
    Status { candidate: Option<String> },
    /// Wait for one candidate; a timeout or failed/blocked candidate returns a failing exit code.
    Wait {
        candidate: String,
        #[arg(long, default_value_t = 1800)]
        timeout_seconds: u64,
    },
    /// Requeue only failed/interrupted obligations; successful jobs remain retained.
    Retry {
        candidate: String,
        #[arg(long)]
        job: Vec<String>,
    },
    /// Detach this owner's interest; other requesters keep shared work alive.
    Cancel {
        action: String,
        #[arg(long)]
        owner: String,
    },
    /// Record a review of the exact revision.
    Review {
        revision: String,
        #[arg(long)]
        reviewer: String,
    },
    /// Atomically advance the validated frontier with complete evidence and an exact review.
    Promote {
        candidate: String,
        #[arg(long)]
        expected: Option<String>,
    },
    /// Copy retained artifacts into a new directory, bound to their original revision.
    Deliver {
        action: String,
        #[arg(long)]
        destination: PathBuf,
    },
    /// Show a project's validated candidate and its certificate.
    Frontier { project: String },
    /// Inspect failures that need repair.
    Repairs,
    /// Record a passing repair candidate, preserving the original failure.
    Resolve {
        repair: String,
        #[arg(long)]
        candidate: String,
    },
    /// Reconcile the local executor while no worker runs. Supervisor loss retains capacity until
    /// an operator confirms drain.
    Recover {
        #[arg(long)]
        confirm_drained: Option<String>,
    },
    /// Read the durable event stream for lane orchestration and measurements.
    Events {
        #[arg(long, default_value_t = 0)]
        after: i64,
    },
    /// Read one immutable attempt and its output.
    Logs { attempt: String },
    /// Export a revision to an explicitly named local bare repository using a ref compare-and-swap.
    Export {
        revision: String,
        #[arg(long)]
        destination: PathBuf,
        #[arg(long, default_value = "refs/heads/main")]
        reference: String,
        #[arg(long)]
        expected: Option<String>,
    },
    /// Show source revisions, private workspaces or registered policies.
    List {
        #[arg(value_parser=["projects","revisions","workspaces","profiles"])]
        kind: String,
    },
    /// Supervise one claimed attempt from its control files; never opens the store.
    #[command(name = "_supervise", hide = true)]
    Supervise { root: PathBuf, attempt: String },
}

#[derive(Args)]
struct Connection {
    #[arg(long)]
    endpoint: String,
    #[arg(long)]
    credential_file: PathBuf,
    #[arg(long)]
    ca_cert: Option<PathBuf>,
}
impl Connection {
    fn open(&self) -> Result<client::Remote> {
        client::Remote::new(
            &self.endpoint,
            &self.credential_file,
            self.ca_cert.as_deref(),
        )
    }
}
#[derive(Subcommand)]
enum RemoteOperation {
    Query {
        #[arg(long)]
        data: String,
    },
    Command {
        #[arg(long)]
        data: String,
        #[arg(long)]
        key: String,
    },
    Events {
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        follow: bool,
    },
    Publish {
        #[arg(long)]
        project: String,
        #[arg(long)]
        bundle: PathBuf,
        #[arg(long)]
        commit: String,
        #[arg(long)]
        message: String,
        #[arg(long)]
        key: String,
    },
    Source {
        revision: String,
        #[arg(long)]
        destination: PathBuf,
    },
    Deliver {
        action: String,
        #[arg(long)]
        destination: PathBuf,
    },
}
fn remote(connection: &Connection, operation: &RemoteOperation) -> Result<Value> {
    let client = connection.open()?;
    let value = match operation {
        RemoteOperation::Query { data } => client.query_value(&serde_json::from_str(data)?)?,
        RemoteOperation::Command { data, key } => {
            client.command_value(key, &serde_json::from_str(data)?)?
        }
        RemoteOperation::Events {
            after,
            project,
            follow,
        } => {
            if *follow {
                client.follow(*after, project.as_deref())?;
                return Ok(Value::Null);
            }
            client.query_value(&api::Query::Events {
                after: *after,
                project: project.clone(),
                limit: 1000,
            })?
        }
        RemoteOperation::Publish {
            project,
            bundle,
            commit,
            message,
            key,
        } => client.publish(key, project, bundle, commit, message)?,
        RemoteOperation::Source {
            revision,
            destination,
        } => client.bundle(revision, destination)?,
        RemoteOperation::Deliver {
            action,
            destination,
        } => client.deliver(action, destination)?,
    };
    if value.get("error").is_some() {
        bail!("{value}");
    }
    Ok(value)
}

fn run(cli: Cli) -> Result<Value> {
    match &cli.command {
        Operation::Serve {
            listen,
            tls_cert,
            tls_key,
            ready_file,
            allow_origin,
        } => {
            return http::serve(
                &cli.state,
                *listen,
                tls_cert.clone(),
                tls_key.clone(),
                ready_file.clone(),
                allow_origin.iter().cloned().collect(),
            );
        }
        Operation::Remote {
            connection,
            operation,
        } => return remote(connection, operation),
        Operation::Supervise { root, attempt } => return worker::supervise(root, attempt),
        Operation::Executor {
            connection,
            work,
            drain,
            poll_ms,
            heartbeat_seconds,
            min_free_gb,
            cas_max_gb,
            retain_hours,
            loopback_allow,
            no_loopback_check,
        } => {
            return executor::remote(
                || connection.open(),
                work,
                executor::Options {
                    drain: *drain,
                    poll: Duration::from_millis(*poll_ms),
                    heartbeat: Duration::from_secs(*heartbeat_seconds),
                },
                executor::Bounds {
                    min_free: min_free_gb * GIB,
                    cas_max: cas_max_gb * GIB,
                    retain: Duration::from_secs(retain_hours * 3600),
                },
                (!no_loopback_check).then(|| loopback_allow.iter().copied().collect()),
            );
        }
        Operation::Mcp { .. } => unreachable!("stdio is handled before JSON output"),
        _ => {}
    }
    if let Operation::Init { slots, memory_mb } = cli.command {
        let store = Store::init(&cli.state, slots, memory_mb)?;
        return Ok(json!({"state":store.root,"slots":slots,"memory_mb":memory_mb}));
    }
    let mut store = Store::open(&cli.state)?;
    Ok(match cli.command {
        Operation::Init { .. } => unreachable!(),
        Operation::Serve { .. }
        | Operation::Remote { .. }
        | Operation::Mcp { .. }
        | Operation::Executor { .. }
        | Operation::Supervise { .. } => unreachable!(),
        Operation::Grant {
            policy,
            credential_file,
        } => api::grant(
            &store,
            serde_json::from_slice(&fs::read(policy)?)?,
            &credential_file,
        )?,
        Operation::Priority { owner, priority } => worker::set_priority(&store, &owner, priority)?,
        Operation::Reserve {
            executor,
            slots,
            memory_mb,
        } => authority::set_reserve(
            &mut store,
            &executor,
            model::Reservation { slots, memory_mb },
        )?,
        Operation::Policy { policy } => {
            api::update_policy(&store, serde_json::from_slice(&fs::read(policy)?)?)?
        }
        Operation::Revoke { principal } => api::revoke(&store, &principal)?,
        Operation::Import {
            name,
            repo,
            rev,
            owner,
        } => serde_json::to_value(source::import(&mut store, &name, &repo, &rev, &owner)?)?,
        Operation::Workspace { revision, owner } => {
            serde_json::to_value(source::workspace(&mut store, &revision, &owner)?)?
        }
        Operation::Checkpoint { workspace, message } => {
            serde_json::to_value(source::checkpoint(&mut store, &workspace, &message)?)?
        }
        Operation::Compose {
            base,
            change,
            owner,
            message,
        } => serde_json::to_value(source::compose(
            &mut store, &base, &change, &owner, &message,
        )?)?,
        Operation::Register { profile } => {
            serde_json::to_value(engine::register(&mut store, &profile)?)?
        }
        Operation::Request {
            revision,
            profile,
            job,
            owner,
        } => serde_json::to_value(engine::request(
            &mut store, &revision, &profile, &job, &owner,
        )?)?,
        Operation::Submit {
            revision,
            profile,
            owner,
            requires,
            retain_from,
        } => serde_json::to_value(engine::submit_retaining(
            &mut store,
            &revision,
            &profile,
            &owner,
            requires,
            retain_from.as_deref(),
        )?)?,
        Operation::Worker {
            drain,
            poll_ms,
            heartbeat_seconds,
            min_free_gb,
            loopback_allow,
            no_loopback_check,
        } => executor::local(
            &store,
            executor::Options {
                drain,
                poll: Duration::from_millis(poll_ms),
                heartbeat: Duration::from_secs(heartbeat_seconds),
            },
            executor::Guards {
                min_free: min_free_gb * GIB,
                loopback: (!no_loopback_check).then(|| loopback_allow.into_iter().collect()),
                ..executor::Guards::default()
            },
        )?,
        Operation::ExecutorSet {
            id,
            slots,
            memory_mb,
            lease_seconds,
            allow_job,
            deny_job,
            clear_placement,
        } => authority::configure(
            &mut store,
            &id,
            &authority::ConfigureRequest {
                slots,
                memory_mb,
                lease_seconds,
                allow: allow_job,
                deny: deny_job,
                clear_placement,
            },
        )?,
        Operation::Executors => authority::list(&store)?,
        Operation::Status { candidate } => match candidate {
            Some(candidate) => engine::candidate_status(&store, &candidate)?,
            None => engine::status(&store)?,
        },
        Operation::Wait {
            candidate,
            timeout_seconds,
        } => {
            let deadline = Instant::now();
            loop {
                let status = engine::candidate_status(&store, &candidate)?;
                match status["state"].as_str() {
                    Some("passed") => break status,
                    Some("failed" | "blocked" | "canceled" | "incomplete") => bail!("{}", status),
                    _ => {}
                }
                if deadline.elapsed().as_secs() >= timeout_seconds {
                    bail!("wait timed out: {status}");
                }
                thread::sleep(Duration::from_millis(250));
            }
        }
        Operation::Retry { candidate, job } => engine::retry(&mut store, &candidate, &job)?,
        Operation::Cancel { action, owner } => engine::cancel(&mut store, &action, &owner)?,
        Operation::Review { revision, reviewer } => {
            engine::review(&mut store, &revision, &reviewer)?
        }
        Operation::Promote {
            candidate,
            expected,
        } => engine::promote(&mut store, &candidate, expected.as_deref())?,
        Operation::Deliver {
            action,
            destination,
        } => worker::deliver(&store, &action, &destination)?,
        Operation::Frontier { project } => engine::frontier(&store, &project)?
            .unwrap_or_else(|| json!({"candidate":null,"certificate":null})),
        Operation::Repairs => serde_json::to_value(engine::repairs(&store)?)?,
        Operation::Resolve { repair, candidate } => {
            engine::resolve_repair(&mut store, &repair, &candidate)?
        }
        Operation::Recover { confirm_drained } => {
            executor::recover(&mut store, confirm_drained.as_deref())?
        }
        Operation::Events { after } => serde_json::to_value(store.events(after)?)?,
        Operation::Logs { attempt } => {
            let row = store.attempt(&attempt)?;
            let path = store.root.join("attempts").join(&row.id).join("output.log");
            json!({"attempt":row,"output":fs::read_to_string(path).unwrap_or_default()})
        }
        Operation::Export {
            revision,
            destination,
            reference,
            expected,
        } => source::export(
            &store,
            &revision,
            &destination,
            &reference,
            expected.as_deref(),
        )?,
        Operation::List { kind } => {
            if kind == "projects" {
                return source::projects(&store);
            }
            // clap constrains kind to these three static internal table names.
            let mut stmt = store
                .db
                .prepare(&format!("SELECT body FROM {kind} ORDER BY rowid"))?;
            let values = stmt
                .query_map([], |r| r.get::<_, String>(0))?
                .map(|v| Ok(serde_json::from_str::<Value>(&v?)?))
                .collect::<Result<Vec<_>>>()?;
            json!(values)
        }
    })
}

fn main() {
    let cli = Cli::parse();
    if let Operation::Mcp { connection } = &cli.command {
        if let Err(error) = connection.open().and_then(|client| mcp::stdio(&client)) {
            eprintln!("{}", json!({"error":format!("{error:#}")}));
            std::process::exit(1);
        }
        return;
    }
    match run(cli) {
        Ok(result) => println!(
            "{}",
            serde_json::to_string_pretty(&result).expect("JSON output")
        ),
        Err(error) => {
            eprintln!("{}", json!({"error":format!("{error:#}")}));
            std::process::exit(1);
        }
    }
}
