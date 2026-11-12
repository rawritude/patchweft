use crate::{model::*, now};
use anyhow::{Result, bail};
use rusqlite::{
    Connection, OptionalExtension, Savepoint, Transaction, TransactionBehavior, params,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    fs,
    ops::Deref,
    path::{Path, PathBuf},
    time::Duration,
};

const SCHEMA: &str = "
CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE projects (id TEXT PRIMARY KEY, name TEXT UNIQUE NOT NULL, origin TEXT NOT NULL);
CREATE TABLE revisions (id TEXT PRIMARY KEY, project TEXT NOT NULL, body TEXT NOT NULL);
CREATE TABLE workspaces (id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE profiles (id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE actions (id TEXT PRIMARY KEY, key TEXT NOT NULL, state TEXT NOT NULL, attempt TEXT,
 body TEXT NOT NULL, created REAL NOT NULL);
CREATE INDEX action_key ON actions(key);
CREATE TABLE interests (action TEXT NOT NULL, owner TEXT NOT NULL, context TEXT NOT NULL,
 active INTEGER NOT NULL, PRIMARY KEY(action,owner,context));
CREATE TABLE attempts (id TEXT PRIMARY KEY, action TEXT NOT NULL, state TEXT NOT NULL,
 drained INTEGER NOT NULL DEFAULT 0, body TEXT NOT NULL);
CREATE TABLE candidates (id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE reviews (revision TEXT NOT NULL, reviewer TEXT NOT NULL, created REAL NOT NULL,
 PRIMARY KEY(revision,reviewer));
CREATE TABLE repairs (id TEXT PRIMARY KEY, revision TEXT NOT NULL, action TEXT,
 kind TEXT NOT NULL, state TEXT NOT NULL, detail TEXT NOT NULL, resolved_by TEXT, created REAL NOT NULL);
CREATE TABLE frontiers (project TEXT PRIMARY KEY, candidate TEXT NOT NULL, certificate TEXT NOT NULL);
CREATE TABLE dispatches (owner TEXT PRIMARY KEY, last REAL NOT NULL);
CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, time REAL NOT NULL,
 kind TEXT NOT NULL, subject TEXT NOT NULL, detail TEXT NOT NULL);
";

pub struct Store {
    pub root: PathBuf,
    pub db: Connection,
}

/// Preserve immediate transactions for local commands and use savepoints when the
/// API owns an outer transaction containing both effects and its durable receipt.
pub enum WriteTransaction<'a> {
    Immediate(Transaction<'a>),
    Nested(Savepoint<'a>),
}

impl Deref for WriteTransaction<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        match self {
            Self::Immediate(transaction) => transaction,
            Self::Nested(savepoint) => savepoint,
        }
    }
}

impl WriteTransaction<'_> {
    pub fn commit(self) -> rusqlite::Result<()> {
        match self {
            Self::Immediate(transaction) => transaction.commit(),
            Self::Nested(savepoint) => savepoint.commit(),
        }
    }
}

impl Store {
    /// Compose existing operations under one rollback boundary without retaining
    /// a Rust borrow of the connection across the callback.
    pub fn atomic<T>(&mut self, operation: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let outer = self.db.is_autocommit();
        self.db.execute_batch(if outer {
            "BEGIN IMMEDIATE"
        } else {
            "SAVEPOINT core_operation"
        })?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(self)));
        match result {
            Ok(Ok(value)) => {
                if let Err(error) = self.db.execute_batch(if outer {
                    "COMMIT"
                } else {
                    "RELEASE core_operation"
                }) {
                    let _ = self.db.execute_batch(if outer {
                        "ROLLBACK"
                    } else {
                        "ROLLBACK TO core_operation; RELEASE core_operation"
                    });
                    return Err(error.into());
                }
                Ok(value)
            }
            Ok(Err(error)) => {
                self.db.execute_batch(if outer {
                    "ROLLBACK"
                } else {
                    "ROLLBACK TO core_operation; RELEASE core_operation"
                })?;
                Err(error)
            }
            Err(panic) => {
                let _ = self.db.execute_batch(if outer {
                    "ROLLBACK"
                } else {
                    "ROLLBACK TO core_operation; RELEASE core_operation"
                });
                std::panic::resume_unwind(panic)
            }
        }
    }

    pub fn init(root: &Path, slots: u32, memory_mb: u32) -> Result<Self> {
        if slots == 0 || memory_mb < 128 {
            bail!("invalid capacity");
        }
        fs::create_dir_all(root)?;
        let root = root.canonicalize()?;
        if root.join("state.sqlite").exists() {
            bail!("state already exists");
        }
        let db = Connection::open(root.join("state.sqlite"))?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.execute_batch(SCHEMA)?;
        for (key, value) in [
            ("schema", "1".to_owned()),
            ("slots", slots.to_string()),
            ("memory_mb", memory_mb.to_string()),
        ] {
            db.execute("INSERT INTO settings VALUES (?,?)", params![key, value])?;
        }
        for directory in ["repos", "workspaces", "attempts", "cache"] {
            fs::create_dir(root.join(directory))?;
        }
        drop(db);
        Self::open(&root)
    }

    pub fn open(root: &Path) -> Result<Self> {
        let root = root.canonicalize()?;
        if !root.join("state.sqlite").is_file() {
            bail!("state is not initialized");
        }
        let db = Connection::open(root.join("state.sqlite"))?;
        db.busy_timeout(Duration::from_secs(30))?;
        db.pragma_update(None, "foreign_keys", "ON")?;
        Ok(Self { root, db })
    }

    pub fn transaction(&mut self) -> Result<WriteTransaction<'_>> {
        if self.db.is_autocommit() {
            Ok(WriteTransaction::Immediate(
                self.db
                    .transaction_with_behavior(TransactionBehavior::Immediate)?,
            ))
        } else {
            Ok(WriteTransaction::Nested(self.db.savepoint()?))
        }
    }

    pub fn capacity(&self) -> Result<(u32, u32)> {
        capacity(&self.db)
    }

    pub fn revision(&self, id: &str) -> Result<Revision> {
        record(&self.db, "revisions", id)
    }
    pub fn workspace(&self, id: &str) -> Result<Workspace> {
        record(&self.db, "workspaces", id)
    }
    pub fn profile(&self, id: &str) -> Result<RegisteredProfile> {
        record(&self.db, "profiles", id)
    }
    pub fn action(&self, id: &str) -> Result<Action> {
        record(&self.db, "actions", id)
    }
    pub fn attempt(&self, id: &str) -> Result<Attempt> {
        record(&self.db, "attempts", id)
    }
    pub fn candidate(&self, id: &str) -> Result<Candidate> {
        record(&self.db, "candidates", id)
    }

    pub fn events(&self, after: i64) -> Result<Vec<Value>> {
        let mut statement = self.db.prepare(
            "SELECT seq,time,kind,subject,detail FROM events WHERE seq>? ORDER BY seq LIMIT 1000",
        )?;
        let rows = statement.query_map([after], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, f64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ))
        })?;
        rows.map(|r| {
            let (seq, time, kind, subject, detail) = r?;
            Ok(json!({"seq":seq,"time":time,"kind":kind,"subject":subject,
                      "detail":serde_json::from_str::<Value>(&detail)?}))
        })
        .collect()
    }
}

/// The local executor's budget; also readable inside an open transaction.
pub fn capacity(db: &Connection) -> Result<(u32, u32)> {
    let get = |key| -> Result<u32> {
        let value: String = db.query_row("SELECT value FROM settings WHERE key=?", [key], |r| {
            r.get(0)
        })?;
        Ok(value.parse()?)
    };
    Ok((get("slots")?, get("memory_mb")?))
}

/// Whether the store has the table: one an older binary never made, or a later one added.
pub fn table_exists(db: &Connection, name: &str) -> Result<bool> {
    Ok(db.query_row(
        "SELECT COUNT(*)>0 FROM sqlite_master WHERE type='table' AND name=?",
        [name],
        |r| r.get(0),
    )?)
}

/// Passed keys of declared jobs' actions. Additive and idempotent; the store schema itself is
/// unchanged. `passed_keys` keeps its v29 shape, so a v29 binary still reads and writes it after
/// a rollback; when each key was last used, and the keys retention retired, are side tables.
pub fn prepare_passed_keys(db: &Connection) -> Result<()> {
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS passed_keys (action TEXT PRIMARY KEY, key TEXT NOT NULL);
         CREATE INDEX IF NOT EXISTS passed_key ON passed_keys(key);
         CREATE TABLE IF NOT EXISTS passed_key_uses (action TEXT PRIMARY KEY, last_used REAL NOT NULL);
         CREATE TABLE IF NOT EXISTS retired_keys (action TEXT PRIMARY KEY, key TEXT NOT NULL,
          retired REAL NOT NULL);",
    )?;
    Ok(())
}

/// Records that a request resolved to `action`'s passed key at `time`. Without a passed key it
/// does nothing.
pub fn touch_passed_key(db: &Connection, action: &str, time: f64) -> Result<()> {
    db.execute(
        "INSERT INTO passed_key_uses SELECT action,? FROM passed_keys WHERE action=?
         ON CONFLICT(action) DO UPDATE SET last_used=MAX(last_used,excluded.last_used)",
        params![time, action],
    )?;
    Ok(())
}

/// The passed key recorded for an action; None for every action outside a declared job's
/// declared path, and in a store that has none. A key retention retired still answers here: it
/// is never reused again, but it stays the identity a reused receipt is checked against.
pub fn passed_key_of(db: &Connection, action: &str) -> Result<Option<String>> {
    if !table_exists(db, "passed_keys")? {
        return Ok(None);
    }
    let key: Option<String> = db
        .query_row(
            "SELECT key FROM passed_keys WHERE action=?",
            [action],
            |r| r.get(0),
        )
        .optional()?;
    if key.is_some() || !table_exists(db, "retired_keys")? {
        return Ok(key);
    }
    Ok(db
        .query_row(
            "SELECT key FROM retired_keys WHERE action=?",
            [action],
            |r| r.get(0),
        )
        .optional()?)
}

// The table identifiers below are internal constants, never accepted from a caller.
pub fn record<T: DeserializeOwned>(db: &Connection, table: &str, id: &str) -> Result<T> {
    let body: Option<String> = db
        .query_row(&format!("SELECT body FROM {table} WHERE id=?"), [id], |r| {
            r.get(0)
        })
        .optional()?;
    match body {
        Some(body) => Ok(serde_json::from_str(&body)?),
        None => bail!("unknown {table} record: {id}"),
    }
}

pub fn insert(db: &Connection, table: &str, id: &str, value: &impl Serialize) -> Result<()> {
    db.execute(
        &format!("INSERT INTO {table}(id,body) VALUES (?,?)"),
        params![id, serde_json::to_string(value)?],
    )?;
    Ok(())
}

pub fn event(db: &Connection, kind: &str, subject: &str, detail: &impl Serialize) -> Result<()> {
    db.execute(
        "INSERT INTO events(time,kind,subject,detail) VALUES (?,?,?,?)",
        params![now(), kind, subject, serde_json::to_string(detail)?],
    )?;
    Ok(())
}
