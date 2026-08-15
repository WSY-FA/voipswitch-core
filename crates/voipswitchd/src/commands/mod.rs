mod dtmf;
mod parser;
mod show;
mod status;
mod vc_help;

use crate::app::AppState;
use crate::pbx;
use anyhow::{Result, anyhow};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use voipswitch_core::command_service::{
    ApiCommand, CliSessionContext, Command, CommandRenderData, CommandRequest,
    CommandRequestPayload, CommandResponse, CommandResult, ConfigCommand,
};
use voipswitch_core::ipc::server::ControlCommandHandler;
use voipswitch_core::types::ids::DomainId;

pub trait ApiCommandHandler: Send + Sync {
    fn name(&self) -> &str;
    fn handle(&self, state: &AppState, command: &ApiCommand) -> Result<CommandResult>;
}

pub trait ConfigCommandHandler: Send + Sync {
    fn name(&self) -> &str;
    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<CommandResult>;
}

#[derive(Clone, Default)]
pub struct ApiCommandRegistry {
    handlers: Arc<RwLock<BTreeMap<String, Arc<dyn ApiCommandHandler>>>>,
}

impl ApiCommandRegistry {
    pub fn register(&self, handler: Arc<dyn ApiCommandHandler>) {
        self.handlers
            .write()
            .expect("api command registry lock poisoned")
            .insert(handler.name().to_string(), handler);
    }

    pub fn dispatch(&self, state: &AppState, command: &ApiCommand) -> Result<CommandResult> {
        let handlers = self
            .handlers
            .read()
            .expect("api command registry lock poisoned");
        let handler = handlers
            .get(&command.name)
            .ok_or_else(|| anyhow!("unsupported api command: {}", command.name))?;
        handler.handle(state, command)
    }
}

#[derive(Clone, Default)]
pub struct ConfigCommandRegistry {
    handlers: Arc<RwLock<BTreeMap<String, Arc<dyn ConfigCommandHandler>>>>,
}

impl ConfigCommandRegistry {
    pub fn register(&self, handler: Arc<dyn ConfigCommandHandler>) {
        self.handlers
            .write()
            .expect("config command registry lock poisoned")
            .insert(handler.name().to_string(), handler);
    }

    pub fn dispatch(&self, state: &AppState, command: &ConfigCommand) -> Result<CommandResult> {
        let handlers = self
            .handlers
            .read()
            .expect("config command registry lock poisoned");
        let handler = handlers
            .get(&command.resource)
            .ok_or_else(|| anyhow!("unsupported config resource: {}", command.resource))?;
        handler.handle(state, command)
    }

    pub fn names(&self) -> Vec<String> {
        self.handlers
            .read()
            .expect("config command registry lock poisoned")
            .keys()
            .cloned()
            .collect()
    }
}

pub struct CommandService {
    state: AppState,
    api_registry: ApiCommandRegistry,
    config_registry: ConfigCommandRegistry,
}

impl CommandService {
    pub fn new(state: AppState, pbx_services: &pbx::PbxServices) -> Arc<Self> {
        let api_registry = ApiCommandRegistry::default();
        let config_registry = ConfigCommandRegistry::default();

        register_core_api_commands(&api_registry);
        pbx::init_modules(
            state.clone(),
            &pbx_services.analysis,
            &config_registry,
            &api_registry,
        );

        Arc::new(Self {
            state,
            api_registry,
            config_registry,
        })
    }

    fn execute_inner(&self, command: Command) -> Result<CommandResult> {
        match command {
            Command::Status => Ok(status::status(&self.state, &self.config_registry)),
            Command::ConfigCheck => Ok(status::config_check(&self.state, &self.config_registry)),
            Command::Api(command) => self.api_registry.dispatch(&self.state, &command),
            Command::Config(command) => self.config_registry.dispatch(&self.state, &command),
        }
    }
}

impl ControlCommandHandler for CommandService {
    fn execute(&self, request: CommandRequest) -> CommandResponse {
        let command = match command_from_payload(request.payload) {
            Ok(command) => command,
            Err(message) => {
                return CommandResponse::error(request.request_id, "INVALID_ARGUMENT", message);
            }
        };

        match self.execute_inner(command) {
            Ok(result) => CommandResponse::ok(request.request_id, result),
            Err(err) => command_error_response(request.request_id, err),
        }
    }

    fn execute_session(
        &self,
        context: &mut CliSessionContext,
        request: CommandRequest,
    ) -> CommandResponse {
        let request_id = request.request_id;
        match request.payload {
            CommandRequestPayload::RawLine { line } => {
                self.execute_cli_line(context, request_id, line)
            }
            payload => {
                let command = match command_from_payload_with_context(payload, context) {
                    Ok(command) => command,
                    Err(message) => {
                        return CommandResponse::error(request_id, "INVALID_ARGUMENT", message)
                            .with_prompt(context.prompt());
                    }
                };
                match self.execute_inner(command) {
                    Ok(result) => {
                        CommandResponse::ok(request_id, result).with_prompt(context.prompt())
                    }
                    Err(err) => {
                        command_error_response(request_id, err).with_prompt(context.prompt())
                    }
                }
            }
        }
    }
}

impl CommandService {
    fn execute_cli_line(
        &self,
        context: &mut CliSessionContext,
        request_id: String,
        line: String,
    ) -> CommandResponse {
        let line = line.trim();
        if line.is_empty() {
            return CommandResponse::ok(request_id, text_result("", Vec::new()))
                .with_prompt(context.prompt());
        }

        if let Some(response) = self.handle_slash_command(context, request_id.clone(), line) {
            return response;
        }

        let words = parser::split_command_line(line);
        let command = match command_from_words_with_context(&words, context) {
            Ok(command) => command,
            Err(message) => {
                return CommandResponse::error(request_id, "INVALID_ARGUMENT", message)
                    .with_prompt(context.prompt());
            }
        };

        match self.execute_inner(command) {
            Ok(result) => CommandResponse::ok(request_id, result).with_prompt(context.prompt()),
            Err(err) => command_error_response(request_id, err).with_prompt(context.prompt()),
        }
    }

    fn handle_slash_command(
        &self,
        context: &mut CliSessionContext,
        request_id: String,
        line: &str,
    ) -> Option<CommandResponse> {
        let words = parser::split_command_line(line);
        let cmd = words.first()?.as_str();
        match cmd {
            "/exit" | "/quit" => Some(
                CommandResponse::ok(request_id, text_result("bye", vec!["bye".to_string()]))
                    .with_prompt(context.prompt())
                    .exit(),
            ),
            "/back" => {
                context.domain_id = None;
                Some(
                    CommandResponse::ok(
                        request_id,
                        text_result("domain cleared", vec!["domain cleared".to_string()]),
                    )
                    .with_prompt(context.prompt()),
                )
            }
            "/domain" => Some(self.handle_domain_slash(context, request_id, &words)),
            "/help" => Some(
                CommandResponse::ok(
                    request_id,
                    text_result(
                        "help",
                        vec![
                            "vc config select domain".to_string(),
                            "/domain <domain_id>".to_string(),
                            "/back".to_string(),
                            "/exit".to_string(),
                        ],
                    ),
                )
                .with_prompt(context.prompt()),
            ),
            _ if cmd.starts_with('/') => Some(
                CommandResponse::error(request_id, "unsupported_slash_command", cmd.to_string())
                    .with_prompt(context.prompt()),
            ),
            _ => None,
        }
    }

    fn handle_domain_slash(
        &self,
        context: &mut CliSessionContext,
        request_id: String,
        words: &[String],
    ) -> CommandResponse {
        match words {
            [_] => {
                if let Some(domain_id) = &context.domain_id {
                    return CommandResponse::ok(
                        request_id,
                        text_result(
                            "current domain",
                            vec![format!("current domain: {domain_id}")],
                        ),
                    )
                    .with_prompt(context.prompt());
                }

                let config = self.state.config().snapshot();
                if config.domains.len() == 1 {
                    let domain_id = config
                        .domains
                        .keys()
                        .next()
                        .cloned()
                        .expect("domain exists");
                    context.domain_id = Some(domain_id.clone());
                    return CommandResponse::ok(
                        request_id,
                        text_result("domain selected", vec![format!("domain: {domain_id}")]),
                    )
                    .with_prompt(context.prompt());
                }

                CommandResponse::error(request_id, "domain_required", "usage: /domain <domain_id>")
                    .with_prompt(context.prompt())
            }
            [_, domain] => {
                let domain_id = DomainId::from(domain.as_str());
                if !self
                    .state
                    .config()
                    .snapshot()
                    .domains
                    .contains_key(&domain_id)
                {
                    return CommandResponse::error(
                        request_id,
                        "domain_not_found",
                        format!("domain not found: {domain_id}"),
                    )
                    .with_prompt(context.prompt());
                }
                context.domain_id = Some(domain_id.clone());
                CommandResponse::ok(
                    request_id,
                    text_result("domain selected", vec![format!("domain: {domain_id}")]),
                )
                .with_prompt(context.prompt())
            }
            _ => CommandResponse::error(
                request_id,
                "invalid_domain_command",
                "usage: /domain <domain_id>",
            )
            .with_prompt(context.prompt()),
        }
    }
}

fn command_from_payload(payload: CommandRequestPayload) -> Result<Command, String> {
    match payload {
        CommandRequestPayload::Parsed { command } => Ok(command),
        CommandRequestPayload::RawWords { words } => parser::parse_command_words(&words),
        CommandRequestPayload::RawLine { line } => {
            parser::parse_command_words(&parser::split_command_line(&line))
        }
    }
}

fn command_from_payload_with_context(
    payload: CommandRequestPayload,
    context: &CliSessionContext,
) -> Result<Command, String> {
    match payload {
        CommandRequestPayload::Parsed { command } => Ok(command),
        CommandRequestPayload::RawWords { words } => {
            command_from_words_with_context(&words, context)
        }
        CommandRequestPayload::RawLine { line } => {
            command_from_words_with_context(&parser::split_command_line(&line), context)
        }
    }
}

fn command_from_words_with_context(
    words: &[String],
    context: &CliSessionContext,
) -> Result<Command, String> {
    let mut command = parser::parse_command_words(words)?;
    apply_cli_domain_context(&mut command, context);
    Ok(command)
}

fn apply_cli_domain_context(command: &mut Command, context: &CliSessionContext) {
    let Some(domain_id) = context.domain_id.clone() else {
        return;
    };
    match command {
        Command::Config(config) if config.domain_id.is_none() => {
            config.domain_id = Some(domain_id);
        }
        Command::Api(api) if api.domain_id.is_none() => {
            api.domain_id = Some(domain_id);
        }
        _ => {}
    }
}

pub(super) fn text_result(message: impl Into<String>, lines: Vec<String>) -> CommandResult {
    CommandResult {
        code: "OK".to_string(),
        message: message.into(),
        data: CommandRenderData::TextLines { lines },
        warnings: Vec::new(),
    }
}

fn command_error_response(request_id: String, err: anyhow::Error) -> CommandResponse {
    let error = err.to_string();
    let (code, message) = structured_error(&error).unwrap_or(("INTERNAL_ERROR", error.as_str()));
    CommandResponse::error(request_id, code, message)
}

fn structured_error(error: &str) -> Option<(&str, &str)> {
    let (code, message) = error.split_once(':')?;
    if code.is_empty()
        || !code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte == b'_')
    {
        return None;
    }
    Some((code, message.trim()))
}

pub(super) fn object_result(message: impl Into<String>, value: serde_json::Value) -> CommandResult {
    CommandResult::object(message, value)
}

fn register_core_api_commands(registry: &ApiCommandRegistry) {
    registry.register(Arc::new(dtmf::DtmfApiCommand));
    registry.register(Arc::new(show::ShowApiCommand));
    registry.register(Arc::new(vc_help::VcHelpApiCommand));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_structured_command_error_codes() {
        assert_eq!(
            structured_error("RESOURCE_NOT_FOUND: domain domain-a"),
            Some(("RESOURCE_NOT_FOUND", "domain domain-a"))
        );
        assert_eq!(structured_error("database unavailable"), None);
    }
}
