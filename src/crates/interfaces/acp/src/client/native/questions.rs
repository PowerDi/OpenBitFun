//! Interactive agent requests shown on the `AskUserQuestion` card.
//!
//! Codex `request_user_input`, MCP elicitation forms and Pi extension dialogs
//! have no native card. The drivers project a synthetic `AskUserQuestion`
//! card, ask through `backend-event-acppermissionrequest` (the same event as
//! Claude's question tool), and settle the card themselves once answered.
//! The card answers with `answers` keyed by question index; "Skip" rejects.
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use serde_json::{json, Value};

use crate::client::SubmitAcpPermissionResponseRequest;

/// Card name the web UI renders as the question form.
pub(crate) const QUESTION_TOOL: &str = "AskUserQuestion";
/// Card name for requests shown as approvals or unsupported notices.
pub(crate) const INTERACTION_TOOL: &str = "AgentInteraction";

/// One question of the card.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Question {
    pub header: String,
    pub question: String,
    /// (label, description). Empty: free text only.
    pub options: Vec<(String, String)>,
    pub multi_select: bool,
}

impl Question {
    pub(crate) fn free_text(header: &str, question: &str) -> Self {
        Self {
            header: header.to_string(),
            question: question.to_string(),
            options: Vec::new(),
            multi_select: false,
        }
    }
}

/// How a synthetic card ends.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CardOutcome {
    Completed { result: Value, text: String },
    Failed(String),
}

/// The user's decision on a question card.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Decision {
    /// Answers per question, in question order (one value unless multi-select).
    Answered(Vec<Vec<String>>),
    Skipped,
}

/// `AskUserQuestion` input for the card.
pub(crate) fn card_input(questions: &[Question]) -> Value {
    json!({"questions": questions.iter().map(|q| json!({
        "header": q.header,
        "question": q.question,
        "multiSelect": q.multi_select,
        "options": q.options.iter()
            .map(|(label, description)| json!({"label": label, "description": description}))
            .collect::<Vec<_>>(),
    })).collect::<Vec<_>>()})
}

fn option(id: &str, name: &str, kind: &str) -> Value {
    json!({"optionId": id, "name": name, "kind": kind})
}

/// Permission request for a question card: the card submits the answers, the
/// approval bar only skips (same options as Claude's question tool).
pub(crate) fn question_payload(
    permission_id: &str,
    session_id: &str,
    card_id: &str,
    title: &str,
    input: &Value,
) -> Value {
    json!({"permissionId": permission_id, "sessionId": session_id, "toolCall": {
        "toolCallId": card_id, "title": title, "kind": "other", "toolName": QUESTION_TOOL,
        "status": "pending", "rawInput": input, "content": []},
        "options": [option("reject_once", "Skip", "reject_once")]})
}

/// Permission request for a yes/no interaction (e.g. an MCP confirmation).
pub(crate) fn confirm_payload(
    permission_id: &str,
    session_id: &str,
    card_id: &str,
    title: &str,
    input: &Value,
) -> Value {
    json!({"permissionId": permission_id, "sessionId": session_id, "toolCall": {
    "toolCallId": card_id, "title": title, "kind": "other", "toolName": INTERACTION_TOOL,
    "status": "pending", "rawInput": input, "content": []},
    "options": [
        option("allow_once", "Accept", "allow_once"),
        option("reject_once", "Decline", "reject_once"),
    ]})
}

/// Approve (`true`) or reject from the selected option.
pub(crate) fn approved(request: &SubmitAcpPermissionResponseRequest) -> OpenBitFunResult<bool> {
    match request.option_id.as_deref() {
        Some("allow_once") => Ok(true),
        Some("reject_once") => Ok(false),
        None => Ok(request.approve),
        Some(other) => Err(OpenBitFunError::Validation(format!(
            "Unknown question option: {other}"
        ))),
    }
}

/// Parse the card's answers for `questions` (keyed by index, or by question
/// text as Claude's card used to send). Every question needs an answer.
pub(crate) fn decision(
    request: &SubmitAcpPermissionResponseRequest,
    questions: &[Question],
) -> OpenBitFunResult<Decision> {
    if !approved(request)? {
        return Ok(Decision::Skipped);
    }
    let answers = request
        .answers
        .as_ref()
        .and_then(Value::as_object)
        .ok_or_else(|| OpenBitFunError::Validation("Question answers are missing".into()))?;
    let mut out = Vec::with_capacity(questions.len());
    for (index, question) in questions.iter().enumerate() {
        let values: Vec<String> = match answers
            .get(&index.to_string())
            .or_else(|| answers.get(&question.question))
        {
            Some(Value::String(answer)) => vec![answer.trim().to_string()],
            Some(Value::Array(values)) => values
                .iter()
                .filter_map(Value::as_str)
                .map(|value| value.trim().to_string())
                .collect(),
            _ => Vec::new(),
        };
        let values: Vec<String> = values.into_iter().filter(|v| !v.is_empty()).collect();
        if values.is_empty() {
            return Err(OpenBitFunError::Validation(format!(
                "Question is unanswered: {}",
                question.question
            )));
        }
        out.push(values);
    }
    Ok(Decision::Answered(out))
}

/// Completed card showing the answers (the card reads `answers` by index).
pub(crate) fn answered_outcome(questions: &[Question], answers: &[Vec<String>]) -> CardOutcome {
    let mut by_index = serde_json::Map::new();
    let mut lines = Vec::new();
    for (index, (question, values)) in questions.iter().zip(answers).enumerate() {
        let value = if question.multi_select {
            json!(values)
        } else {
            json!(values.join(", "))
        };
        by_index.insert(index.to_string(), value);
        lines.push(format!("{}: {}", question.question, values.join(", ")));
    }
    CardOutcome::Completed {
        result: json!({"answers": by_index}),
        text: lines.join("\n"),
    }
}

/// One answer that must be one of the offered option labels.
pub(crate) fn single_choice<'a>(
    question: &Question,
    values: &'a [String],
) -> OpenBitFunResult<&'a str> {
    let answer = values.first().map(String::as_str).unwrap_or_default();
    if question.options.iter().any(|(label, _)| label == answer) {
        Ok(answer)
    } else {
        Err(OpenBitFunError::Validation(format!(
            "Choose one of the offered options for: {}",
            question.question
        )))
    }
}

/// What one MCP elicitation form field accepts.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FieldKind {
    Text,
    Number {
        integer: bool,
    },
    Boolean,
    /// Option labels map to these values (same order).
    Choice(Vec<String>),
    Multi(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Field {
    name: String,
    kind: FieldKind,
    question: Question,
}

const YES: &str = "Yes";
const NO: &str = "No";

/// An MCP elicitation the question card can answer.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Elicitation {
    /// No fields: accept or decline (e.g. an MCP tool approval).
    Confirm,
    Form(Vec<Field>),
}

impl Elicitation {
    /// Map an MCP `requestedSchema` (flat object of primitive fields, MCP
    /// 2025-11-25). `Err` names what the card cannot show.
    pub(crate) fn parse(message: &str, schema: &Value) -> Result<Self, String> {
        let properties = match schema.get("properties") {
            None | Some(Value::Null) => return Ok(Self::Confirm),
            Some(Value::Object(properties)) if properties.is_empty() => return Ok(Self::Confirm),
            Some(Value::Object(properties)) => properties,
            Some(_) => return Err("the form schema has no field list".into()),
        };
        let mut fields = Vec::with_capacity(properties.len());
        for (index, (name, field)) in properties.iter().enumerate() {
            let title = field["title"].as_str().unwrap_or(name).to_string();
            let mut prompt = field["description"].as_str().unwrap_or(&title).to_string();
            if index == 0 && !message.trim().is_empty() {
                prompt = format!("{}\n\n{prompt}", message.trim());
            }
            let labelled = |options: &[Value], value_key: &str, label_key: &str| {
                options
                    .iter()
                    .map(|option| {
                        let value = option[value_key].as_str()?.to_string();
                        let label = option[label_key].as_str().unwrap_or(&value).to_string();
                        Some((label, value))
                    })
                    .collect::<Option<Vec<_>>>()
            };
            let plain = |values: &Value, names: &Value| {
                values.as_array().and_then(|values| {
                    values
                        .iter()
                        .enumerate()
                        .map(|(i, value)| {
                            let value = value.as_str()?.to_string();
                            let label = names[i].as_str().unwrap_or(&value).to_string();
                            Some((label, value))
                        })
                        .collect::<Option<Vec<_>>>()
                })
            };
            let (kind, options, multi_select) = match field["type"].as_str() {
                Some("string") if field.get("oneOf").is_some() => {
                    let pairs = field["oneOf"]
                        .as_array()
                        .and_then(|options| labelled(options, "const", "title"))
                        .ok_or_else(|| format!("field '{name}' has unreadable options"))?;
                    choice(pairs, false)
                }
                Some("string") if field.get("enum").is_some() => {
                    let pairs = plain(&field["enum"], &field["enumNames"])
                        .ok_or_else(|| format!("field '{name}' has unreadable options"))?;
                    choice(pairs, false)
                }
                Some("string") => (FieldKind::Text, Vec::new(), false),
                Some(kind @ ("number" | "integer")) => (
                    FieldKind::Number {
                        integer: kind == "integer",
                    },
                    Vec::new(),
                    false,
                ),
                Some("boolean") => (
                    FieldKind::Boolean,
                    vec![
                        (YES.to_string(), String::new()),
                        (NO.to_string(), String::new()),
                    ],
                    false,
                ),
                Some("array") => {
                    let items = &field["items"];
                    let pairs = match items.get("anyOf").and_then(Value::as_array) {
                        Some(options) => labelled(options, "const", "title"),
                        None => plain(&items["enum"], &Value::Null),
                    }
                    .ok_or_else(|| format!("field '{name}' has unreadable options"))?;
                    choice(pairs, true)
                }
                other => {
                    return Err(format!(
                        "field '{name}' has unsupported type {}",
                        other.unwrap_or("(none)")
                    ))
                }
            };
            fields.push(Field {
                name: name.clone(),
                kind,
                question: Question {
                    header: title,
                    question: prompt,
                    options,
                    multi_select,
                },
            });
        }
        Ok(Self::Form(fields))
    }

    pub(crate) fn questions(&self) -> Vec<Question> {
        match self {
            Self::Confirm => Vec::new(),
            Self::Form(fields) => fields.iter().map(|f| f.question.clone()).collect(),
        }
    }

    /// `content` of an accepted elicitation from the card's answers.
    pub(crate) fn content(&self, answers: &[Vec<String>]) -> OpenBitFunResult<Value> {
        let Self::Form(fields) = self else {
            return Ok(json!({}));
        };
        let invalid = |field: &Field, what: &str| {
            OpenBitFunError::Validation(format!("{}: {what}", field.question.header))
        };
        let mut content = serde_json::Map::new();
        for (field, values) in fields.iter().zip(answers) {
            let first = values.first().map(String::as_str).unwrap_or_default();
            let value = match &field.kind {
                FieldKind::Text => json!(first),
                FieldKind::Number { integer: true } => json!(first
                    .parse::<i64>()
                    .map_err(|_| invalid(field, "enter a whole number"))?),
                FieldKind::Number { integer: false } => json!(first
                    .parse::<f64>()
                    .map_err(|_| invalid(field, "enter a number"))?),
                FieldKind::Boolean => match first {
                    YES => json!(true),
                    NO => json!(false),
                    _ => return Err(invalid(field, "choose Yes or No")),
                },
                FieldKind::Choice(choices) => {
                    let label = single_choice(&field.question, values)?;
                    json!(choice_value(&field.question, choices, label))
                }
                FieldKind::Multi(choices) => {
                    let mut out = Vec::with_capacity(values.len());
                    for label in values {
                        if !field.question.options.iter().any(|(l, _)| l == label) {
                            return Err(invalid(field, "choose only offered options"));
                        }
                        out.push(choice_value(&field.question, choices, label));
                    }
                    json!(out)
                }
            };
            content.insert(field.name.clone(), value);
        }
        Ok(Value::Object(content))
    }
}

fn choice(pairs: Vec<(String, String)>, multi: bool) -> (FieldKind, Vec<(String, String)>, bool) {
    let values = pairs.iter().map(|(_, value)| value.clone()).collect();
    let options = pairs
        .into_iter()
        .map(|(label, _)| (label, String::new()))
        .collect();
    let kind = if multi {
        FieldKind::Multi(values)
    } else {
        FieldKind::Choice(values)
    };
    (kind, options, multi)
}

fn choice_value(question: &Question, values: &[String], label: &str) -> String {
    question
        .options
        .iter()
        .position(|(l, _)| l == label)
        .and_then(|index| values.get(index))
        .cloned()
        .unwrap_or_else(|| label.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elicitation_forms_map_to_questions_and_back() {
        let schema = json!({"type":"object","properties":{
            "color":{"type":"string","title":"Color","oneOf":[{"const":"r","title":"Red"},{"const":"g","title":"Green"}]},
            "count":{"type":"integer","description":"How many?"},
            "ok":{"type":"boolean"},
            "tags":{"type":"array","items":{"type":"string","enum":["a","b"]}}
        },"required":["color"]});
        let form = Elicitation::parse("Pick", &schema).unwrap();
        let questions = form.questions();
        assert_eq!(questions.len(), 4);
        assert_eq!(questions[0].question, "Pick\n\nColor");
        assert_eq!(questions[0].options[1].0, "Green");
        assert!(questions[3].multi_select);
        let content = form
            .content(&[
                vec!["Green".into()],
                vec!["3".into()],
                vec!["No".into()],
                vec!["b".into()],
            ])
            .unwrap();
        assert_eq!(
            content,
            json!({"color":"g","count":3,"ok":false,"tags":["b"]})
        );
        assert!(form
            .content(&[
                vec!["Blue".into()],
                vec!["3".into()],
                vec!["No".into()],
                vec!["b".into()],
            ])
            .is_err());
        assert_eq!(
            Elicitation::parse("", &json!({"type":"object","properties":{}})).unwrap(),
            Elicitation::Confirm
        );
        assert!(Elicitation::parse(
            "",
            &json!({"type":"object","properties":{"x":{"type":"object"}}})
        )
        .is_err());
    }

    fn request(option: Option<&str>, answers: Option<Value>) -> SubmitAcpPermissionResponseRequest {
        SubmitAcpPermissionResponseRequest {
            permission_id: "p".into(),
            approve: option != Some("reject_once"),
            option_id: option.map(str::to_string),
            answers,
            feedback: None,
        }
    }

    #[test]
    fn decision_reads_index_and_text_keys_and_requires_every_answer() {
        let questions = vec![
            Question::free_text("A", "First?"),
            Question {
                multi_select: true,
                ..Question::free_text("B", "Second?")
            },
        ];
        let answered = decision(
            &request(
                Some("allow_once"),
                Some(json!({"0": " one ", "Second?": ["x", "", "y"]})),
            ),
            &questions,
        )
        .unwrap();
        assert_eq!(
            answered,
            Decision::Answered(vec![vec!["one".into()], vec!["x".into(), "y".into()]])
        );
        assert!(decision(
            &request(Some("allow_once"), Some(json!({"0": "one"}))),
            &questions
        )
        .is_err());
        assert_eq!(
            decision(&request(Some("reject_once"), None), &questions).unwrap(),
            Decision::Skipped
        );
        let Decision::Answered(answers) = answered else {
            unreachable!()
        };
        let CardOutcome::Completed { result, .. } = answered_outcome(&questions, &answers) else {
            panic!("completed");
        };
        assert_eq!(result, json!({"answers": {"0": "one", "1": ["x", "y"]}}));
    }
}
