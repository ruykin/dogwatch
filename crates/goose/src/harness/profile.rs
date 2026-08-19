//! Harness profiles: control-plane-served bundles that provision a session.
//!
//! A profile is pure data — exam and org deployments differ only in profile
//! content, never in harness code. Profiles load from a local path or an
//! HTTP(S) URL (the control plane).

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::policy::HarnessPolicy;
use super::IngestConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    /// Stable identifier, stamped on every event (e.g. `exam/backend-2026-q3`).
    pub id: String,
    /// Human description shown at bootstrap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Remote ingest endpoint. Local JSONL is always written regardless.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingest: Option<IngestConfig>,
    /// Tool policy enforced on both native and ACP paths.
    #[serde(default)]
    pub policy: HarnessPolicy,
    /// Config keys pinned for the session. Applied as process environment
    /// variables at bootstrap, which sit at the top of goose's config
    /// precedence — so they win over the user's config.yaml.
    #[serde(default)]
    pub config_locked: BTreeMap<String, String>,
    /// Workspace materialization (exam exercises). Absent for org profiles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<WorkspaceSpec>,
    /// Recipe to launch the session with (name or path, passed to `goose run`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipe: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceSpec {
    /// Git URL to clone as the working repo.
    pub repo: String,
    /// Directory name for the clone (defaults to the repo name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<String>,
    /// Branch or ref to check out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,
}

impl Profile {
    pub fn parse(raw: &str) -> Result<Self> {
        serde_yaml::from_str(raw).context("parsing harness profile YAML")
    }

    pub fn load_from_path(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading profile {}", path.display()))?;
        Self::parse(&raw)
    }

    pub async fn load_from_url(url: &str, token: Option<&str>) -> Result<Self> {
        let client = reqwest::Client::new();
        let mut request = client.get(url);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("fetching profile from {url}"))?
            .error_for_status()
            .with_context(|| format!("fetching profile from {url}"))?;
        let raw = response.text().await.context("reading profile body")?;
        Self::parse(&raw)
    }

    /// Load from a local path or an http(s) URL.
    pub async fn load(source: &str, token: Option<&str>) -> Result<Self> {
        if source.starts_with("http://") || source.starts_with("https://") {
            Self::load_from_url(source, token).await
        } else {
            Self::load_from_path(Path::new(source))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_profile() {
        let yaml = r#"
id: exam/backend-2026-q3
description: Backend take-home exam
ingest:
  endpoint: https://ingest.example.com/v1/events
  token: tok_abc
policy:
  deny: ["*production*"]
  require_approval: ["developer__shell"]
config_locked:
  GOOSE_MODE: approve
workspace:
  repo: https://github.com/example/exercise.git
  dir: exercise
recipe: exam.yaml
"#;
        let profile = Profile::parse(yaml).unwrap();
        assert_eq!(profile.id, "exam/backend-2026-q3");
        assert_eq!(profile.config_locked["GOOSE_MODE"], "approve");
        assert_eq!(profile.policy.require_approval, vec!["developer__shell"]);
        assert_eq!(profile.workspace.unwrap().dir.as_deref(), Some("exercise"));
    }

    #[test]
    fn minimal_profile_defaults() {
        let profile = Profile::parse("id: org/default").unwrap();
        assert!(profile.ingest.is_none());
        assert!(profile.policy.is_empty());
        assert!(profile.config_locked.is_empty());
    }
}
