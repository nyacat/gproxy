//! A lower-bound text estimate. Transport framing and opaque media are not tokens.
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(crate) struct ResponsesMeter {
    segments: BTreeMap<(u64, u64, &'static str, u64), u64>,
    sequences: BTreeSet<(u64, u64)>,
    responses: BTreeMap<String, u64>,
    response: u64,
}

impl ResponsesMeter {
    pub(crate) fn observe(&mut self, event: &Value) {
        if let Some(id) = event
            .get("response_id")
            .or_else(|| event.pointer("/response/id"))
            .and_then(Value::as_str)
        {
            // Slot zero also owns deltas received before response.created/id.
            // Learning the first ID must not double-count a final snapshot.
            let next = self.responses.len() as u64;
            self.response = *self.responses.entry(id.to_owned()).or_insert(next);
        }
        if let Some(sequence) = event.get("sequence_number").and_then(Value::as_u64)
            && !self.sequences.insert((self.response, sequence))
        {
            return;
        }
        let kind = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let index = event
            .get("output_index")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let part = event
            .get("content_index")
            .or_else(|| event.get("summary_index"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let segment = match kind {
            "response.output_text.delta" | "response.output_text.done" => Some(("text", "text")),
            "response.reasoning_text.delta" | "response.reasoning_text.done" => {
                Some(("reasoning", "text"))
            }
            "response.reasoning_summary_text.delta" | "response.reasoning_summary_text.done" => {
                Some(("summary", "text"))
            }
            "response.refusal.delta" | "response.refusal.done" => Some(("refusal", "refusal")),
            "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
                Some(("arguments", "arguments"))
            }
            "response.custom_tool_call_input.delta" | "response.custom_tool_call_input.done" => {
                Some(("input", "input"))
            }
            "response.mcp_call_arguments.delta" | "response.mcp_call_arguments.done" => {
                Some(("arguments", "arguments"))
            }
            "response.code_interpreter_call_code.delta"
            | "response.code_interpreter_call_code.done" => Some(("code", "code")),
            "response.audio.transcript.delta" | "response.audio.transcript.done" => {
                Some(("transcript", "transcript"))
            }
            _ => None,
        };
        if let Some((segment, final_key)) = segment {
            let delta = kind.ends_with(".delta");
            self.text(
                index,
                segment,
                part,
                event.get(if delta { "delta" } else { final_key }),
                delta,
            );
        }
        if kind == "response.output_item.done"
            && let Some(item) = event.get("item")
        {
            self.item(index, item);
        }
        if matches!(
            kind,
            "response.completed" | "response.failed" | "response.incomplete"
        ) && let Some(output) = event.pointer("/response/output").and_then(Value::as_array)
        {
            for (index, item) in output.iter().enumerate() {
                self.item(index as u64, item);
            }
        }
    }

    fn item(&mut self, index: u64, item: &Value) {
        match item.get("type").and_then(Value::as_str) {
            Some("message" | "reasoning") => {
                for (array, fallback) in [("content", "text"), ("summary", "summary")] {
                    if let Some(parts) = item.get(array).and_then(Value::as_array) {
                        for (part, value) in parts.iter().enumerate() {
                            let kind = match value.get("type").and_then(Value::as_str) {
                                Some("reasoning_text") => "reasoning",
                                Some("refusal") => "refusal",
                                Some("summary_text") => "summary",
                                Some("output_text" | "text") => fallback,
                                _ => continue,
                            };
                            self.text(
                                index,
                                kind,
                                part as u64,
                                value.get(if kind == "refusal" { "refusal" } else { "text" }),
                                false,
                            );
                        }
                    }
                }
            }
            Some("function_call" | "mcp_call") => {
                self.text(index, "arguments", 0, item.get("arguments"), false)
            }
            Some("custom_tool_call") => self.text(index, "input", 0, item.get("input"), false),
            Some("code_interpreter_call") => self.text(index, "code", 0, item.get("code"), false),
            _ => {}
        }
    }

    fn text(
        &mut self,
        index: u64,
        kind: &'static str,
        part: u64,
        value: Option<&Value>,
        delta: bool,
    ) {
        if let Some(text) = value.and_then(Value::as_str) {
            let chars = text.chars().count() as u64;
            let total = self
                .segments
                .entry((self.response, index, kind, part))
                .or_default();
            *total = if delta {
                total.saturating_add(chars)
            } else {
                (*total).max(chars)
            };
        }
    }

    pub(crate) fn characters(&self) -> u64 {
        self.segments
            .values()
            .fold(0u64, |sum, chars| sum.saturating_add(*chars))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deltas_done_and_final_snapshots_count_each_content_once() {
        let mut meter = ResponsesMeter::default();
        let delta = json!({"type":"response.output_text.delta", "output_index":0, "delta":"你好hello", "sequence_number":1});
        meter.observe(&delta);
        meter.observe(&delta);
        meter.observe(
            &json!({"type":"response.output_text.done", "output_index":0, "text":"你好hello"}),
        );
        meter.observe(&json!({"type":"response.function_call_arguments.delta", "output_index":1, "delta":"{\"x\":1}"}));
        meter.observe(&json!({"type":"response.completed", "response":{"output":[
            {"type":"message", "content":[{"type":"output_text", "text":"你好hello"}]},
            {"type":"function_call", "arguments":"{\"x\":1}"},
            {"type":"reasoning", "summary":[{"type":"summary_text", "text":"think"}], "encrypted_content":"opaque string that is not output tokens"}
        ]}}));
        assert_eq!(meter.characters(), 7 + 7 + 5);
    }

    #[test]
    fn response_ids_scope_sequences_and_late_identity_reuses_partial_content() {
        let mut meter = ResponsesMeter::default();
        meter.observe(&json!({"type":"response.output_text.delta", "output_index":0, "delta":"hello", "sequence_number":1}));
        meter.observe(&json!({"type":"response.completed", "response":{"id":"first", "output":[{"type":"message", "content":[{"type":"output_text", "text":"hello"}]}]}}));
        assert_eq!(meter.characters(), 5);
        meter.observe(&json!({"type":"response.created", "response":{"id":"second"}}));
        meter.observe(&json!({"type":"response.output_text.delta", "output_index":0, "delta":"again", "sequence_number":1}));
        assert_eq!(meter.characters(), 10);
    }
}
