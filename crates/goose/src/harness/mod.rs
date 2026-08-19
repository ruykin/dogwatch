//! Agent harness: identity, observability, and policy for harness-managed sessions.
//!
//! A session becomes harness-managed when a harness context is discoverable:
//! either `GOOSE_HARNESS_CONTEXT` points at a `session.json`, or a
//! `.goose-harness/session.json` exists in the current directory or any
//! ancestor. When no context is found the harness is inactive and every tap
//! is a cheap no-op.
//!
//! See `HARNESS_ARCHITECTURE.md` at the repo root for the full design.

pub mod events;
pub mod policy;
pub mod profile;
pub mod sink;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

use crate::conversation::message::Message;
use events::HarnessEvent;
use policy::HarnessPolicy;

/// Environment variable pointing at the harness context file (`session.json`).
pub const HARNESS_CONTEXT_ENV: &str = "GOOSE_HARNESS_CONTEXT";
/// Directory name used for workspace-local harness state.
pub const HARNESS_DIR: &str = ".goose-harness";
/// Context file name inside [`HARNESS_DIR`].
pub const CONTEXT_FILE: &str = "session.json";
/// Local JSONL event log file name inside [`HARNESS_DIR`].
pub const EVENTS_FILE: &str = "events.jsonl";

/// Remote ingest endpoint configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestConfig {
    /// URL that accepts `POST` with a JSON array of events.
    pub endpoint: String,
    /// Bearer token attached to each batch (exam token / org credential).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

/// The durable identity + configuration of one harness session, persisted at
/// `.goose-harness/session.json` in the workspace so that every goose process
/// launched inside the workspace attaches to the same harness session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessContext {
    /// Harness-minted session id, stamped on every event. Independent of both
    /// goose and ACP session ids (which are recorded on events when known).
    pub harness_session_id: String,
    /// The identified human behind the session (candidate id, SSO principal).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    /// Profile that provisioned this session (e.g. `exam/backend-2026-q3`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    /// Workspace root (where `.goose-harness/` lives).
    pub workspace: PathBuf,
    /// Git commit the workspace started from, for `submit` diffs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_commit: Option<String>,
    /// Remote ingest endpoint; local JSONL is always written regardless.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingest: Option<IngestConfig>,
    /// Tool policy applied on both the native and ACP paths.
    #[serde(default)]
    pub policy: HarnessPolicy,
}

impl HarnessContext {
    pub fn harness_dir(&self) -> PathBuf {
        self.workspace.join(HARNESS_DIR)
    }

    pub fn context_path(&self) -> PathBuf {
        self.harness_dir().join(CONTEXT_FILE)
    }

    pub fn events_path(&self) -> PathBuf {
        self.harness_dir().join(EVENTS_FILE)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading harness context {}", path.display()))?;
        serde_json::from_str(&raw)
            .with_context(|| format!("parsing harness context {}", path.display()))
    }

    pub fn save(&self) -> Result<()> {
        let dir = self.harness_dir();
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("creating {}", dir.display()))?;
        let path = self.context_path();
        let raw = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, raw)
            .with_context(|| format!("writing harness context {}", path.display()))?;
        Ok(())
    }
}

/// Live harness state for this process: the context plus the event pump.
pub struct Harness {
    ctx: HarnessContext,
    seq: AtomicU64,
    sink: sink::SinkHandle,
}

impl Harness {
    fn new(ctx: HarnessContext) -> Self {
        let sink = sink::SinkHandle::start(&ctx);
        Self {
            ctx,
            seq: AtomicU64::new(0),
            sink,
        }
    }

    pub fn context(&self) -> &HarnessContext {
        &self.ctx
    }

    pub fn policy(&self) -> &HarnessPolicy {
        &self.ctx.policy
    }

    fn envelope(
        &self,
        kind: &str,
        goose_session_id: Option<&str>,
        payload: serde_json::Value,
    ) -> HarnessEvent {
        HarnessEvent {
            v: events::SCHEMA_VERSION,
            harness_session_id: self.ctx.harness_session_id.clone(),
            principal: self.ctx.principal.clone(),
            profile_id: self.ctx.profile_id.clone(),
            goose_session_id: goose_session_id.map(str::to_string),
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
            ts: chrono::Utc::now().to_rfc3339(),
            kind: kind.to_string(),
            payload,
        }
    }

    /// Emit a single event. Never blocks; failures are logged, not raised.
    pub fn emit(&self, kind: &str, goose_session_id: Option<&str>, payload: serde_json::Value) {
        let event = self.envelope(kind, goose_session_id, payload);
        self.sink.send(event);
    }

    /// Derive and emit events for one persisted message.
    pub fn record_message(&self, goose_session_id: &str, message: &Message) {
        for (kind, payload) in events::events_for_message(message) {
            self.emit(&kind, Some(goose_session_id), payload);
        }
    }

    /// Flush buffered events (local file + remote batches). Best effort.
    pub async fn flush(&self) {
        self.sink.flush().await;
    }
}

static HARNESS: OnceLock<Option<Arc<Harness>>> = OnceLock::new();

/// Locate the context file: explicit env var first, then walk up from cwd.
fn discover_context_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var(HARNESS_CONTEXT_ENV) {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
        tracing::warn!(
            "{} is set but {} does not exist; harness inactive",
            HARNESS_CONTEXT_ENV,
            path.display()
        );
        return None;
    }
    let mut dir = std::env::current_dir().ok()?;
    loop {
        let candidate = dir.join(HARNESS_DIR).join(CONTEXT_FILE);
        if candidate.is_file() {
            return Some(candidate);
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// The active harness for this process, if any. Discovery runs once and is
/// cached; when no context is found this is a cheap `None`.
pub fn active() -> Option<Arc<Harness>> {
    HARNESS
        .get_or_init(|| {
            let path = discover_context_path()?;
            match HarnessContext::load(&path) {
                Ok(ctx) => {
                    tracing::info!(
                        harness_session_id = %ctx.harness_session_id,
                        "harness active (context: {})",
                        path.display()
                    );
                    Some(Arc::new(Harness::new(ctx)))
                }
                Err(e) => {
                    tracing::warn!("failed to load harness context: {e:#}");
                    None
                }
            }
        })
        .clone()
}

/// Explicitly activate the harness with a freshly created context (bootstrap).
/// Fails if the harness was already resolved for this process.
pub fn activate(ctx: HarnessContext) -> Result<Arc<Harness>> {
    let harness = Arc::new(Harness::new(ctx));
    let stored = harness.clone();
    if HARNESS.set(Some(stored)).is_err() {
        anyhow::bail!("harness already initialized for this process");
    }
    Ok(harness)
}

/// Tap: called after a message is persisted. No-op when inactive.
pub fn record_message(goose_session_id: &str, message: &Message) {
    if let Some(h) = active() {
        h.record_message(goose_session_id, message);
    }
}

/// Tap: token usage recorded for a session. No-op when inactive.
pub fn record_usage(goose_session_id: &str, model: &str, usage_json: serde_json::Value) {
    if let Some(h) = active() {
        h.emit(
            "usage",
            Some(goose_session_id),
            serde_json::json!({ "model": model, "usage": usage_json }),
        );
    }
}

/// Tap: a permission decision was made (either path). No-op when inactive.
pub fn record_permission_decision(
    goose_session_id: Option<&str>,
    subject: &str,
    decision: &str,
    source: &str,
) {
    if let Some(h) = active() {
        h.emit(
            "permission_decision",
            goose_session_id,
            serde_json::json!({
                "subject": subject,
                "decision": decision,
                "source": source,
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = HarnessContext {
            harness_session_id: "hs-test".into(),
            principal: Some("cand-42".into()),
            profile_id: Some("exam/demo".into()),
            workspace: dir.path().to_path_buf(),
            base_commit: None,
            ingest: None,
            policy: HarnessPolicy::default(),
        };
        ctx.save().unwrap();
        let loaded = HarnessContext::load(&ctx.context_path()).unwrap();
        assert_eq!(loaded.harness_session_id, "hs-test");
        assert_eq!(loaded.principal.as_deref(), Some("cand-42"));
    }
}
