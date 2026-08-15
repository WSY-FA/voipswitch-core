use crate::app::AppState;
use crate::commands::{ApiCommandHandler, object_result};
use crate::runtime::call::dtmf_operation::{
    DtmfOperationMode, DtmfOperationSource, DtmfOperationSpec, all_digits, parse_digit_set,
};
use anyhow::{Result, anyhow};
use serde_json::json;
use std::time::Duration;
use voipswitch_core::command_service::{ApiCommand, CommandResult};

const MAX_COLLECTION_DIGITS: usize = 64;
const MAX_COLLECTION_TIMEOUT_MS: u64 = 600_000;

pub(super) struct DtmfApiCommand;

impl ApiCommandHandler for DtmfApiCommand {
    fn name(&self) -> &str {
        "dtmf"
    }

    fn handle(&self, state: &AppState, command: &ApiCommand) -> Result<CommandResult> {
        let Some(action) = command.args.first().map(String::as_str) else {
            return Err(anyhow!("INVALID_ARGUMENT: dtmf requires collect or cancel"));
        };
        let service = state.dtmf_operations();
        let view = match action {
            "collect" => service
                .start(collection_spec(command)?)
                .map_err(|error| anyhow!(error))?,
            "cancel" => {
                let operation_id = field(command, &["operation", "operation-id", "operation_id"])
                    .ok_or_else(|| {
                    anyhow!("INVALID_ARGUMENT: dtmf cancel requires --operation")
                })?;
                service
                    .cancel(operation_id)
                    .map_err(|error| anyhow!(error))?
            }
            _ => {
                return Err(anyhow!(
                    "INVALID_ARGUMENT: unsupported dtmf action {action}"
                ));
            }
        };
        Ok(object_result(
            "DTMF operation accepted",
            json!({
                "resource": "dtmf_operation",
                "data": view,
            }),
        ))
    }
}

fn collection_spec(command: &ApiCommand) -> Result<DtmfOperationSpec> {
    let call_id = field(command, &["call", "call-id", "call_id"])
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("INVALID_ARGUMENT: dtmf collect requires --call"))?
        .to_string();
    let source = match field(command, &["source"]).unwrap_or("caller") {
        "caller" => DtmfOperationSource::Caller,
        "callee" => DtmfOperationSource::Callee,
        value => {
            return Err(anyhow!("INVALID_ARGUMENT: unsupported DTMF source {value}"));
        }
    };
    let mode = match field(command, &["mode"]).unwrap_or("collect") {
        "observe" => DtmfOperationMode::Observe,
        "collect" => DtmfOperationMode::Collect,
        value => {
            return Err(anyhow!(
                "INVALID_ARGUMENT: unsupported DTMF collection mode {value}"
            ));
        }
    };
    let min_digits = usize_field(command, &["min-digits", "min_digits"], 1)?;
    let max_digits = usize_field(command, &["max-digits", "max_digits"], 1)?;
    if max_digits == 0 || min_digits > max_digits || max_digits > MAX_COLLECTION_DIGITS {
        return Err(anyhow!(
            "INVALID_ARGUMENT: digit range must satisfy min <= max <= {MAX_COLLECTION_DIGITS}"
        ));
    }
    let terminators = parse_digit_set(field(command, &["terminators"]).unwrap_or(""))
        .map_err(|error| anyhow!(error))?;
    Ok(DtmfOperationSpec {
        call_id,
        source,
        mode,
        allowed: all_digits(),
        min_digits,
        max_digits,
        terminators,
        first_digit_timeout: duration_field(
            command,
            &["first-timeout-ms", "first_timeout_ms"],
            5_000,
        )?,
        inter_digit_timeout: duration_field(
            command,
            &["inter-timeout-ms", "inter_timeout_ms"],
            3_000,
        )?,
        overall_timeout: duration_field(
            command,
            &["overall-timeout-ms", "overall_timeout_ms"],
            20_000,
        )?,
    })
}

fn field<'a>(command: &'a ApiCommand, names: &[&str]) -> Option<&'a str> {
    names
        .iter()
        .find_map(|name| command.fields.get(*name).map(String::as_str))
}

fn usize_field(command: &ApiCommand, names: &[&str], default: usize) -> Result<usize> {
    field(command, names)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| anyhow!("INVALID_ARGUMENT: {} must be an integer", names[0]))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn duration_field(command: &ApiCommand, names: &[&str], default_ms: u64) -> Result<Duration> {
    let milliseconds = field(command, names)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| anyhow!("INVALID_ARGUMENT: {} must be an integer", names[0]))
        })
        .transpose()?
        .unwrap_or(default_ms);
    if milliseconds == 0 || milliseconds > MAX_COLLECTION_TIMEOUT_MS {
        return Err(anyhow!(
            "INVALID_ARGUMENT: {} must be 1..={MAX_COLLECTION_TIMEOUT_MS}",
            names[0]
        ));
    }
    Ok(Duration::from_millis(milliseconds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn command(fields: &[(&str, &str)]) -> ApiCommand {
        ApiCommand {
            name: "dtmf".to_string(),
            args: vec!["collect".to_string()],
            domain_id: None,
            key: None,
            fields: fields
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    #[test]
    fn collection_command_defaults_to_one_caller_digit() {
        let spec = collection_spec(&command(&[("call", "call-a")])).unwrap();
        assert_eq!(spec.call_id, "call-a");
        assert_eq!(spec.source, DtmfOperationSource::Caller);
        assert_eq!(spec.mode, DtmfOperationMode::Collect);
        assert_eq!(spec.min_digits, 1);
        assert_eq!(spec.max_digits, 1);
    }

    #[test]
    fn collection_command_rejects_unbounded_values() {
        assert!(collection_spec(&command(&[])).is_err());
        assert!(collection_spec(&command(&[("call", "a"), ("max-digits", "65")])).is_err());
        assert!(collection_spec(&command(&[("call", "a"), ("first-timeout-ms", "0")])).is_err());
    }
}
