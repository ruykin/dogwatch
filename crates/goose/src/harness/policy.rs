//! Harness tool policy, enforced on both execution paths:
//!
//! - **Native path** — [`HarnessPolicyInspector`] plugs into goose's
//!   `ToolInspector` pipeline and runs before dispatch.
//! - **ACP path** — externally executed tools never reach the inspector
//!   pipeline, so the ACP provider consults [`HarnessPolicy::check`] when the
//!   external agent raises a permission request (see `acp/provider.rs`).
//!
//! Patterns match case-insensitively. A pattern without `*` must match the
//! subject exactly; `*` acts as a glob wildcard. Native subjects are full
//! tool names (`developer__shell`); ACP subjects are the permission request
//! title, since ACP agents don't expose goose tool names.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::GooseMode;
use crate::conversation::message::{Message, ToolRequest};
use crate::tool_inspection::{InspectionAction, InspectionResult, ToolInspector};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HarnessPolicy {
    /// Tools matching these patterns are refused outright.
    #[serde(default)]
    pub deny: Vec<String>,
    /// Tools matching these patterns always require human approval, even in
    /// full-auto modes.
    #[serde(default)]
    pub require_approval: Vec<String>,
}

/// Verdict for a single subject against the policy. Deny wins over
/// require-approval when both match.
#[derive(Debug, Clone, PartialEq)]
pub enum PolicyVerdict {
    Allow,
    RequireApproval(String),
    Deny(String),
}

impl HarnessPolicy {
    pub fn is_empty(&self) -> bool {
        self.deny.is_empty() && self.require_approval.is_empty()
    }

    pub fn check(&self, subject: &str) -> PolicyVerdict {
        if let Some(p) = self.deny.iter().find(|p| pattern_matches(p, subject)) {
            return PolicyVerdict::Deny(format!("blocked by harness policy (rule: {p})"));
        }
        if let Some(p) = self
            .require_approval
            .iter()
            .find(|p| pattern_matches(p, subject))
        {
            return PolicyVerdict::RequireApproval(format!(
                "approval required by harness policy (rule: {p})"
            ));
        }
        PolicyVerdict::Allow
    }
}

/// Case-insensitive match; `*` is a glob wildcard, otherwise exact.
pub fn pattern_matches(pattern: &str, subject: &str) -> bool {
    let pattern = pattern.to_lowercase();
    let subject = subject.to_lowercase();
    if !pattern.contains('*') {
        return pattern == subject;
    }
    let segments: Vec<&str> = pattern.split('*').collect();
    let mut rest = subject.as_str();
    for (i, segment) in segments.iter().enumerate() {
        if segment.is_empty() {
            continue;
        }
        if i == 0 {
            // Anchored start.
            match rest.strip_prefix(segment) {
                Some(r) => rest = r,
                None => return false,
            }
        } else if i == segments.len() - 1 {
            // Anchored end.
            return rest.ends_with(segment);
        } else {
            match rest.find(segment) {
                Some(pos) => rest = rest.split_at(pos + segment.len()).1,
                None => return false,
            }
        }
    }
    true
}

/// Native-path enforcement: a `ToolInspector` fed by the active harness
/// policy. Registered unconditionally; a no-op when the harness is inactive
/// or the policy is empty.
pub struct HarnessPolicyInspector;

#[async_trait]
impl ToolInspector for HarnessPolicyInspector {
    fn name(&self) -> &'static str {
        "harness_policy"
    }

    fn is_enabled(&self) -> bool {
        super::active().is_some_and(|h| !h.policy().is_empty())
    }

    async fn inspect(
        &self,
        session_id: &str,
        tool_requests: &[ToolRequest],
        _messages: &[Message],
        _goose_mode: GooseMode,
    ) -> Result<Vec<InspectionResult>> {
        let Some(harness) = super::active() else {
            return Ok(vec![]);
        };
        let policy = harness.policy();
        let mut results = Vec::new();
        for request in tool_requests {
            let Ok(call) = &request.tool_call else {
                continue;
            };
            let subject: &str = call.name.as_ref();
            let (action, reason, decision) = match policy.check(subject) {
                PolicyVerdict::Allow => continue,
                PolicyVerdict::Deny(reason) => (InspectionAction::Deny, reason, "policy_deny"),
                PolicyVerdict::RequireApproval(reason) => (
                    InspectionAction::RequireApproval(Some(reason.clone())),
                    reason,
                    "policy_require_approval",
                ),
            };
            super::record_permission_decision(Some(session_id), subject, decision, "native");
            results.push(InspectionResult {
                tool_request_id: request.id.clone(),
                action,
                reason,
                confidence: 1.0,
                inspector_name: "harness_policy".to_string(),
                finding_id: None,
            });
        }
        Ok(results)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match_without_wildcard() {
        assert!(pattern_matches("developer__shell", "Developer__Shell"));
        assert!(!pattern_matches("shell", "developer__shell"));
    }

    #[test]
    fn glob_wildcards() {
        assert!(pattern_matches("*shell*", "developer__shell"));
        assert!(pattern_matches("developer__*", "developer__text_editor"));
        assert!(pattern_matches("*rm -rf*", "Run `rm -rf /tmp/x`"));
        assert!(!pattern_matches("developer__*", "github__create_pr"));
        assert!(pattern_matches("*", "anything"));
    }

    #[test]
    fn deny_wins_over_require_approval() {
        let policy = HarnessPolicy {
            deny: vec!["*danger*".into()],
            require_approval: vec!["*".into()],
        };
        assert!(matches!(
            policy.check("dangerous_tool"),
            PolicyVerdict::Deny(_)
        ));
        assert!(matches!(
            policy.check("safe_tool"),
            PolicyVerdict::RequireApproval(_)
        ));
    }

    #[test]
    fn empty_policy_allows() {
        let policy = HarnessPolicy::default();
        assert_eq!(policy.check("anything"), PolicyVerdict::Allow);
    }
}
