use std::collections::BTreeMap;
use voipswitch_core::command_service::{ApiCommand, Command, ConfigCommand};
use voipswitch_core::types::ids::DomainId;

pub fn parse_command_words(words: &[String]) -> Result<Command, String> {
    match words {
        [] => Ok(Command::Status),
        [cmd] if cmd == "status" => Ok(Command::Status),
        [cmd] if cmd == "vc" => Ok(Command::Api(parse_api_command("vc", &[])?)),
        [prefix, noun] if prefix == "vc" && noun == "config" => Ok(Command::Api(
            parse_api_command("vc", std::slice::from_ref(noun))?,
        )),
        [resource, action] if resource == "config" && action == "check" => Ok(Command::ConfigCheck),
        [prefix, noun, action, table, rest @ ..] if prefix == "vc" && noun == "config" => Ok(
            Command::Config(parse_vc_config_command(action, table, rest)?),
        ),
        [show, topic] if show == "show" => Ok(Command::Api(parse_api_command(
            show,
            std::slice::from_ref(topic),
        )?)),
        [show, topic, rest @ ..] if show == "show" => {
            let mut argv = vec![topic.clone()];
            argv.extend(rest.iter().cloned());
            Ok(Command::Api(parse_api_command(show, &argv)?))
        }
        [name, rest @ ..] if name == "dtmf" => {
            Ok(Command::Api(parse_named_api_command(name, rest)?))
        }
        [resource, action, rest @ ..] => Ok(Command::Config(parse_config_command(
            resource, action, rest,
        )?)),
        [cmd] => Err(format!("unsupported command: {cmd}")),
    }
}

pub fn split_command_line(line: &str) -> Vec<String> {
    line.split_whitespace().map(str::to_string).collect()
}

fn parse_vc_config_command(
    action: &str,
    table: &str,
    rest: &[String],
) -> Result<ConfigCommand, String> {
    let (domain_id, key, mut fields) = parse_common_args(rest)?;
    fields.insert("table".to_string(), table.to_string());
    Ok(ConfigCommand {
        resource: "vc_config".to_string(),
        action: action.to_string(),
        domain_id,
        key: key.or_else(|| Some(table.to_string())),
        fields,
    })
}

fn parse_api_command(name: &str, argv: &[String]) -> Result<ApiCommand, String> {
    if name == "show" {
        return parse_show_api_command(argv);
    }
    let (domain_id, key, fields) = parse_common_args(argv)?;
    Ok(ApiCommand {
        name: name.to_string(),
        args: argv
            .iter()
            .filter(|item| !item.starts_with("--") && !item.contains('='))
            .cloned()
            .collect(),
        domain_id,
        key,
        fields,
    })
}

fn parse_show_api_command(argv: &[String]) -> Result<ApiCommand, String> {
    parse_named_api_command("show", argv)
}

fn parse_named_api_command(name: &str, argv: &[String]) -> Result<ApiCommand, String> {
    let mut domain_id = None;
    let mut key = None;
    let mut fields = BTreeMap::new();
    let mut args = Vec::new();
    let mut index = 0;
    while index < argv.len() {
        let item = &argv[index];
        if let Some(option) = item.strip_prefix("--") {
            let Some(value) = argv.get(index + 1) else {
                return Err(format!("missing value for --{option}"));
            };
            match option {
                "domain" => domain_id = Some(DomainId::from(value.as_str())),
                "key" | "id" | "number" | "trunk" | "route" => key = Some(value.clone()),
                _ => {
                    fields.insert(option.to_string(), value.clone());
                }
            }
            index += 2;
        } else if let Some((name, value)) = item.split_once('=') {
            fields.insert(name.to_string(), value.to_string());
            index += 1;
        } else {
            args.push(item.clone());
            index += 1;
        }
    }
    Ok(ApiCommand {
        name: name.to_string(),
        args,
        domain_id,
        key,
        fields,
    })
}

fn parse_config_command(
    resource: &str,
    action: &str,
    rest: &[String],
) -> Result<ConfigCommand, String> {
    let (domain_id, key, fields) = parse_common_args(rest)?;
    Ok(ConfigCommand {
        resource: resource.to_string(),
        action: action.to_string(),
        domain_id,
        key,
        fields,
    })
}

type CommonArgs = (Option<DomainId>, Option<String>, BTreeMap<String, String>);

fn parse_common_args(rest: &[String]) -> Result<CommonArgs, String> {
    let mut domain_id = None;
    let mut key = None;
    let mut fields = BTreeMap::new();
    let mut positional = Vec::new();
    let mut index = 0;

    while index < rest.len() {
        let item = &rest[index];
        if let Some(option) = item.strip_prefix("--") {
            let Some(value) = rest.get(index + 1) else {
                return Err(format!("missing value for --{option}"));
            };
            match option {
                "domain" => domain_id = Some(DomainId::from(value.as_str())),
                "key" | "id" | "number" | "trunk" | "route" => key = Some(value.clone()),
                _ => {
                    fields.insert(option.to_string(), value.clone());
                }
            }
            index += 2;
        } else if let Some((name, value)) = item.split_once('=') {
            fields.insert(name.to_string(), value.to_string());
            index += 1;
        } else {
            positional.push(item.clone());
            index += 1;
        }
    }

    if domain_id.is_none() && !positional.is_empty() {
        domain_id = Some(DomainId::from(positional.remove(0)));
    }
    if key.is_none() && !positional.is_empty() {
        key = Some(positional.remove(0));
    }
    for chunk in positional.chunks(2) {
        if let [name, value] = chunk {
            fields.insert(name.clone(), value.clone());
        }
    }

    Ok((domain_id, key, fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn show_topic_is_not_inferred_as_domain() {
        let command =
            parse_command_words(&split_command_line("show cdr --page 1 --page-size 20")).unwrap();
        let Command::Api(command) = command else {
            panic!("expected API command");
        };
        assert_eq!(command.args, vec!["cdr"]);
        assert!(command.domain_id.is_none());
        assert_eq!(command.fields.get("page").map(String::as_str), Some("1"));
    }

    #[test]
    fn show_call_trace_preserves_call_id_as_argument() {
        let command = parse_command_words(&split_command_line("show call-trace call-123")).unwrap();
        let Command::Api(command) = command else {
            panic!("expected API command");
        };
        assert_eq!(command.args, vec!["call-trace", "call-123"]);
        assert!(command.domain_id.is_none());
    }

    #[test]
    fn dtmf_command_keeps_action_positional_and_runtime_fields() {
        let command = parse_command_words(&split_command_line(
            "dtmf collect --call call-123 --source caller --mode collect",
        ))
        .unwrap();
        let Command::Api(command) = command else {
            panic!("expected API command");
        };
        assert_eq!(command.name, "dtmf");
        assert_eq!(command.args, vec!["collect"]);
        assert_eq!(
            command.fields.get("call").map(String::as_str),
            Some("call-123")
        );
        assert!(command.domain_id.is_none());
    }
}
