//! Event sinks: a local JSONL log (always on) plus an optional HTTP ingest
//! endpoint with batching, retry, and a bounded buffer.
//!
//! Emission never blocks the agent loop: events go over an unbounded channel
//! to a background pump. If no tokio runtime is available at start (sync CLI
//! paths), events are appended to the JSONL file synchronously and remote
//! ingest is skipped for that process.

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::events::HarnessEvent;
use super::{HarnessContext, IngestConfig};

/// Flush remote batches at this size…
const BATCH_SIZE: usize = 50;
/// …or at this interval, whichever comes first.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);
/// Maximum events retained for remote delivery while the endpoint is down.
const MAX_REMOTE_BUFFER: usize = 10_000;
/// Per-request timeout for ingest posts.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

enum PumpMsg {
    Event(Box<HarnessEvent>),
    Flush(oneshot::Sender<()>),
}

pub struct SinkHandle {
    tx: Option<mpsc::UnboundedSender<PumpMsg>>,
    /// Fallback used when no tokio runtime exists: direct synchronous append.
    sync_path: Option<Mutex<PathBuf>>,
}

impl SinkHandle {
    pub fn start(ctx: &HarnessContext) -> Self {
        let events_path = ctx.events_path();
        if let Some(dir) = events_path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let (tx, rx) = mpsc::unbounded_channel();
                let pump = Pump {
                    events_path,
                    ingest: ctx.ingest.clone(),
                    remote_buffer: Vec::new(),
                    dropped: 0,
                    client: None,
                };
                handle.spawn(pump.run(rx));
                Self {
                    tx: Some(tx),
                    sync_path: None,
                }
            }
            Err(_) => Self {
                tx: None,
                sync_path: Some(Mutex::new(events_path)),
            },
        }
    }

    pub fn send(&self, event: HarnessEvent) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(PumpMsg::Event(Box::new(event)));
        } else if let Some(path) = &self.sync_path {
            if let Ok(path) = path.lock() {
                append_jsonl(&path, &event);
            }
        }
    }

    pub async fn flush(&self) {
        if let Some(tx) = &self.tx {
            let (ack_tx, ack_rx) = oneshot::channel();
            if tx.send(PumpMsg::Flush(ack_tx)).is_ok() {
                let _ = tokio::time::timeout(Duration::from_secs(15), ack_rx).await;
            }
        }
    }
}

fn append_jsonl(path: &PathBuf, event: &HarnessEvent) {
    let line = match serde_json::to_string(event) {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!("harness: failed to serialize event: {e}");
            return;
        }
    };
    let result = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut f| writeln!(f, "{line}"));
    if let Err(e) = result {
        tracing::warn!("harness: failed to append event log {}: {e}", path.display());
    }
}

struct Pump {
    events_path: PathBuf,
    ingest: Option<IngestConfig>,
    remote_buffer: Vec<HarnessEvent>,
    dropped: u64,
    client: Option<reqwest::Client>,
}

impl Pump {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<PumpMsg>) {
        let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(PumpMsg::Event(event)) => {
                            append_jsonl(&self.events_path, &event);
                            if self.ingest.is_some() {
                                self.buffer_remote(*event);
                                if self.remote_buffer.len() >= BATCH_SIZE {
                                    self.try_post().await;
                                }
                            }
                        }
                        Some(PumpMsg::Flush(ack)) => {
                            self.try_post().await;
                            let _ = ack.send(());
                        }
                        None => {
                            // All senders dropped: final delivery attempt.
                            self.try_post().await;
                            return;
                        }
                    }
                }
                _ = ticker.tick() => {
                    if !self.remote_buffer.is_empty() {
                        self.try_post().await;
                    }
                }
            }
        }
    }

    fn buffer_remote(&mut self, event: HarnessEvent) {
        if self.remote_buffer.len() >= MAX_REMOTE_BUFFER {
            self.remote_buffer.remove(0);
            self.dropped += 1;
            if self.dropped == 1 || self.dropped.is_multiple_of(1000) {
                tracing::warn!(
                    "harness: remote ingest unreachable; {} events dropped from remote buffer (local JSONL log is unaffected)",
                    self.dropped
                );
            }
        }
        self.remote_buffer.push(event);
    }

    async fn try_post(&mut self) {
        let Some(ingest) = &self.ingest else {
            return;
        };
        if self.remote_buffer.is_empty() {
            return;
        }
        let client = self.client.get_or_insert_with(|| {
            reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .unwrap_or_default()
        });
        let mut request = client.post(&ingest.endpoint).json(&self.remote_buffer);
        if let Some(token) = &ingest.token {
            request = request.bearer_auth(token);
        }
        match request.send().await {
            Ok(resp) if resp.status().is_success() => {
                if self.dropped > 0 {
                    tracing::warn!(
                        "harness: ingest recovered; {} events were dropped while it was unreachable",
                        self.dropped
                    );
                    self.dropped = 0;
                }
                self.remote_buffer.clear();
            }
            Ok(resp) => {
                tracing::warn!(
                    "harness: ingest returned {}; retaining {} events for retry",
                    resp.status(),
                    self.remote_buffer.len()
                );
            }
            Err(e) => {
                tracing::warn!(
                    "harness: ingest post failed ({e}); retaining {} events for retry",
                    self.remote_buffer.len()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::events::SCHEMA_VERSION;

    fn event(seq: u64) -> HarnessEvent {
        HarnessEvent {
            v: SCHEMA_VERSION,
            harness_session_id: "hs".into(),
            principal: None,
            profile_id: None,
            goose_session_id: None,
            seq,
            ts: "2026-08-19T00:00:00Z".into(),
            kind: "prompt".into(),
            payload: serde_json::json!({"content": {"text": "hi"}}),
        }
    }

    #[tokio::test]
    async fn events_are_written_to_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = HarnessContext {
            harness_session_id: "hs".into(),
            principal: None,
            profile_id: None,
            workspace: dir.path().to_path_buf(),
            base_commit: None,
            ingest: None,
            policy: Default::default(),
        };
        let handle = SinkHandle::start(&ctx);
        handle.send(event(0));
        handle.send(event(1));
        handle.flush().await;
        let content = std::fs::read_to_string(ctx.events_path()).unwrap();
        let lines: Vec<_> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        let parsed: HarnessEvent = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed.kind, "prompt");
    }

    #[test]
    fn sync_fallback_writes_without_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = HarnessContext {
            harness_session_id: "hs".into(),
            principal: None,
            profile_id: None,
            workspace: dir.path().to_path_buf(),
            base_commit: None,
            ingest: None,
            policy: Default::default(),
        };
        let handle = SinkHandle::start(&ctx);
        handle.send(event(0));
        let content = std::fs::read_to_string(ctx.events_path()).unwrap();
        assert_eq!(content.lines().count(), 1);
    }
}
