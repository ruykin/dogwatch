//! Normalized harness event schema and derivation from goose messages.
//!
//! One `HarnessEvent` is emitted per message content block, so consumers see
//! prompts, agent output, tool calls, tool results, and permission requests
//! as distinct, typed events regardless of whether the executing agent was a
//! native goose provider or an external ACP agent (Claude Code, etc.).

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use rmcp::model::Role;

use crate::conversation::message::{Message, MessageContentBlock};

/// Bump when the envelope or payload shapes change incompatibly.
pub const SCHEMA_VERSION: u32 = 1;

/// Cap applied to large text payload fields (chars).
const TEXT_CAP: usize = 200_000;

/// The envelope every harness event is wrapped in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessEvent {
    pub v: u32,
    pub harness_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goose_session_id: Option<String>,
    /// Monotonic per-process sequence number.
    pub seq: u64,
    /// RFC 3339 timestamp.
    pub ts: String,
    /// Event kind: `session_start`, `prompt`, `agent_message`,
    /// `agent_thinking`, `tool_call`, `tool_result`, `permission_request`,
    /// `permission_decision`, `usage`, `artifact`, `session_end`,
    /// `session_pause`, `system_notification`, `agent_error`, `events_dropped`.
    pub kind: String,
    pub payload: Value,
}

fn cap(s: &str) -> Value {
    if s.chars().count() > TEXT_CAP {
        let truncated: String = s.chars().take(TEXT_CAP).collect();
        json!({ "text": truncated, "truncated": true, "original_chars": s.chars().count() })
    } else {
        json!({ "text": s })
    }
}

fn cap_value(v: &Value) -> Value {
    let raw = v.to_string();
    if raw.chars().count() > TEXT_CAP {
        let truncated: String = raw.chars().take(TEXT_CAP).collect();
        json!({ "json_truncated": truncated, "truncated": true, "original_chars": raw.chars().count() })
    } else {
        v.clone()
    }
}

/// Derive `(kind, payload)` pairs from one persisted message.
pub fn events_for_message(message: &Message) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    let base = |mut payload: serde_json::Map<String, Value>| -> Value {
        payload.insert("role".into(), json!(role_str(&message.role)));
        if let Some(id) = &message.id {
            payload.insert("message_id".into(), json!(id));
        }
        payload.insert("created".into(), json!(message.created));
        if !message.metadata.agent_visible {
            payload.insert("agent_visible".into(), json!(false));
        }
        if !message.metadata.user_visible {
            payload.insert("user_visible".into(), json!(false));
        }
        Value::Object(payload)
    };

    for block in &message.content {
        let (kind, mut payload): (&str, serde_json::Map<String, Value>) = match block {
            MessageContentBlock::Text(t) => {
                let kind = if message.role == Role::User {
                    "prompt"
                } else {
                    "agent_message"
                };
                let mut m = serde_json::Map::new();
                m.insert("content".into(), cap(&t.text));
                (kind, m)
            }
            MessageContentBlock::Thinking(t) => {
                let mut m = serde_json::Map::new();
                m.insert("content".into(), cap(&t.thinking));
                ("agent_thinking", m)
            }
            MessageContentBlock::RedactedThinking(_) => {
                let mut m = serde_json::Map::new();
                m.insert("redacted".into(), json!(true));
                ("agent_thinking", m)
            }
            MessageContentBlock::Image(i) => {
                let mut m = serde_json::Map::new();
                m.insert("mime_type".into(), json!(i.mime_type));
                ("prompt", m)
            }
            MessageContentBlock::ToolRequest(req) => {
                let mut m = serde_json::Map::new();
                m.insert("id".into(), json!(req.id));
                m.insert("external".into(), json!(req.was_executed_externally()));
                match &req.tool_call {
                    Ok(call) => {
                        m.insert("tool_name".into(), json!(call.name.as_ref()));
                        m.insert(
                            "arguments".into(),
                            cap_value(&serde_json::to_value(&call.arguments).unwrap_or(Value::Null)),
                        );
                    }
                    Err(e) => {
                        m.insert("invalid".into(), json!(e.to_string()));
                    }
                }
                ("tool_call", m)
            }
            MessageContentBlock::ToolResponse(resp) => {
                let mut m = serde_json::Map::new();
                m.insert("id".into(), json!(resp.id));
                match &resp.tool_result {
                    Ok(result) => {
                        m.insert("ok".into(), json!(result.is_error != Some(true)));
                        m.insert(
                            "content".into(),
                            cap_value(&serde_json::to_value(result).unwrap_or(Value::Null)),
                        );
                    }
                    Err(e) => {
                        m.insert("ok".into(), json!(false));
                        m.insert("error".into(), json!(e.to_string()));
                    }
                }
                ("tool_result", m)
            }
            MessageContentBlock::ToolConfirmationRequest(req) => {
                let mut m = serde_json::Map::new();
                m.insert("id".into(), json!(req.id));
                m.insert("tool_name".into(), json!(req.tool_name));
                m.insert(
                    "arguments".into(),
                    cap_value(&serde_json::to_value(&req.arguments).unwrap_or(Value::Null)),
                );
                ("permission_request", m)
            }
            MessageContentBlock::ActionRequired(a) => {
                let mut m = serde_json::Map::new();
                m.insert(
                    "action".into(),
                    cap_value(&serde_json::to_value(&a.data).unwrap_or(Value::Null)),
                );
                ("permission_request", m)
            }
            MessageContentBlock::FrontendToolRequest(req) => {
                let mut m = serde_json::Map::new();
                m.insert("id".into(), json!(req.id));
                if let Ok(call) = &req.tool_call {
                    m.insert("tool_name".into(), json!(call.name.as_ref()));
                }
                m.insert("frontend".into(), json!(true));
                ("tool_call", m)
            }
            MessageContentBlock::SystemNotification(n) => {
                let mut m = serde_json::Map::new();
                m.insert(
                    "notification".into(),
                    cap_value(&serde_json::to_value(n).unwrap_or(Value::Null)),
                );
                ("system_notification", m)
            }
            MessageContentBlock::Error(e) => {
                let mut m = serde_json::Map::new();
                m.insert(
                    "error".into(),
                    cap_value(&serde_json::to_value(e).unwrap_or(Value::Null)),
                );
                ("agent_error", m)
            }
        };
        out.push((kind.to_string(), base(std::mem::take(&mut payload))));
    }
    out
}

fn role_str(role: &Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::Assistant => "assistant",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::message::Message;

    #[test]
    fn user_text_is_prompt_and_assistant_text_is_agent_message() {
        let user = Message::user().with_text("hello");
        let events = events_for_message(&user);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "prompt");
        assert_eq!(events[0].1["content"]["text"], "hello");

        let agent = Message::assistant().with_text("hi there");
        let events = events_for_message(&agent);
        assert_eq!(events[0].0, "agent_message");
    }

    #[test]
    fn long_text_is_capped() {
        let long = "x".repeat(TEXT_CAP + 10);
        let msg = Message::user().with_text(&long);
        let events = events_for_message(&msg);
        assert_eq!(events[0].1["content"]["truncated"], true);
    }
}
