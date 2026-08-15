use crate::types::ids::DomainId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRequest {
    pub request_id: String,
    pub payload: CommandRequestPayload,
}

impl CommandRequest {
    pub fn parsed(request_id: impl Into<String>, command: Command) -> Self {
        Self {
            request_id: request_id.into(),
            payload: CommandRequestPayload::Parsed { command },
        }
    }

    pub fn raw_words(request_id: impl Into<String>, words: Vec<String>) -> Self {
        Self {
            request_id: request_id.into(),
            payload: CommandRequestPayload::RawWords { words },
        }
    }

    pub fn raw_line(request_id: impl Into<String>, line: impl Into<String>) -> Self {
        Self {
            request_id: request_id.into(),
            payload: CommandRequestPayload::RawLine { line: line.into() },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandRequestPayload {
    Parsed { command: Command },
    RawWords { words: Vec<String> },
    RawLine { line: String },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CliSessionContext {
    pub domain_id: Option<DomainId>,
}

impl CliSessionContext {
    pub fn prompt(&self) -> String {
        match &self.domain_id {
            Some(domain_id) => format!("voipswitch@{}> ", domain_id.as_str()),
            None => "voipswitch@local> ".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    Status,
    ConfigCheck,
    Api(ApiCommand),
    Config(ConfigCommand),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiCommand {
    pub name: String,
    pub args: Vec<String>,
    pub domain_id: Option<DomainId>,
    pub key: Option<String>,
    pub fields: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigCommand {
    pub resource: String,
    pub action: String,
    pub domain_id: Option<DomainId>,
    pub key: Option<String>,
    pub fields: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
    pub request_id: String,
    pub ok: bool,
    pub result: Option<CommandResult>,
    pub error: Option<CommandError>,
    pub prompt: Option<String>,
    pub exit: bool,
}

impl CommandResponse {
    pub fn ok(request_id: String, result: CommandResult) -> Self {
        Self {
            request_id,
            ok: true,
            result: Some(result),
            error: None,
            prompt: None,
            exit: false,
        }
    }

    pub fn error(request_id: String, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            request_id,
            ok: false,
            result: None,
            error: Some(CommandError {
                code: code.into(),
                message: message.into(),
            }),
            prompt: None,
            exit: false,
        }
    }

    pub fn with_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.prompt = Some(prompt.into());
        self
    }

    pub fn exit(mut self) -> Self {
        self.exit = true;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResult {
    pub code: String,
    pub message: String,
    pub data: CommandRenderData,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "render", content = "payload", rename_all = "snake_case")]
pub enum CommandRenderData {
    TextLines {
        lines: Vec<String>,
    },
    Table {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Kv {
        items: BTreeMap<String, Value>,
    },
    Object {
        value: Value,
    },
}

impl CommandResult {
    pub fn kv(message: impl Into<String>, items: BTreeMap<String, Value>) -> Self {
        Self {
            code: "OK".to_string(),
            message: message.into(),
            data: CommandRenderData::Kv { items },
            warnings: Vec::new(),
        }
    }

    pub fn object(message: impl Into<String>, value: Value) -> Self {
        Self {
            code: "OK".to_string(),
            message: message.into(),
            data: CommandRenderData::Object { value },
            warnings: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandError {
    pub code: String,
    pub message: String,
}
