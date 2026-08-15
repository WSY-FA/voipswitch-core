use anyhow::{Context, Result, bail};
use clap::Parser;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::path::PathBuf;
use tokio::net::UnixStream;
use voipswitch_core::command_service::{CommandRenderData, CommandRequest, CommandResponse};
use voipswitch_core::ipc::frame::{read_json_frame, write_json_frame};
use voipswitch_core::ipc::server::send_command;
use voipswitch_core::types::time::unix_timestamp_ms;

#[derive(Debug, Parser)]
#[command(name = "vs_cli")]
#[command(about = "VoIPSwitch control CLI")]
struct Args {
    #[arg(long)]
    socket: Option<PathBuf>,

    #[arg(short = 'x', long = "execute")]
    execute: Option<String>,

    #[arg(long)]
    json: bool,

    #[arg(trailing_var_arg = true)]
    command: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let socket = args
        .socket
        .clone()
        .unwrap_or_else(|| default_socket_path("control.sock"));

    if args.execute.is_none() && args.command.is_empty() && !args.json {
        return interactive(socket).await;
    }

    let words = command_words(&args);
    let socket = args
        .socket
        .unwrap_or_else(|| default_socket_path("control.sock"));
    let request = CommandRequest::raw_words(format!("cli-{}", unix_timestamp_ms()), words);

    let stream = UnixStream::connect(&socket)
        .await
        .with_context(|| format!("connect {}", socket.display()))?;
    let response = send_command(stream, request).await?;
    render_response(response, args.json)
}

async fn interactive(socket: PathBuf) -> Result<()> {
    let mut stream = UnixStream::connect(&socket)
        .await
        .with_context(|| format!("connect {}", socket.display()))?;
    println!("connected to {}", socket.display());
    println!("type /exit or /quit to leave");
    let mut prompt = "voipswitch@local> ".to_string();
    let mut editor = DefaultEditor::new()?;

    loop {
        let line = match editor.readline(&prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                println!();
                break;
            }
            Err(err) => return Err(err.into()),
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let _ = editor.add_history_entry(line);

        let request = CommandRequest::raw_line(format!("cli-{}", unix_timestamp_ms()), line);
        write_json_frame(&mut stream, &request).await?;
        let response: CommandResponse = read_json_frame(&mut stream).await?;
        if let Some(next_prompt) = response.prompt.clone() {
            prompt = next_prompt;
        }
        let exit = response.exit;
        if let Err(err) = render_response(response, false) {
            eprintln!("error: {err}");
        }
        if exit {
            break;
        }
    }

    Ok(())
}

fn command_words(args: &Args) -> Vec<String> {
    if let Some(command) = &args.execute {
        return command.split_whitespace().map(str::to_string).collect();
    }
    args.command.clone()
}

fn render_response(response: CommandResponse, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }

    if !response.ok {
        let Some(error) = response.error else {
            bail!("command failed");
        };
        bail!("{}: {}", error.code, error.message);
    }

    let Some(result) = response.result else {
        println!("ok");
        return Ok(());
    };

    render_data(&result.data)?;
    for warning in result.warnings {
        eprintln!("warning: {warning}");
    }

    Ok(())
}

fn render_data(data: &CommandRenderData) -> Result<()> {
    match data {
        CommandRenderData::TextLines { lines } => {
            for line in lines {
                println!("{line}");
            }
        }
        CommandRenderData::Table { columns, rows } => render_table(columns, rows),
        CommandRenderData::Kv { items } => {
            for (key, value) in items {
                println!("{key}: {}", render_value(value));
            }
        }
        CommandRenderData::Object { value } => {
            println!("{}", serde_json::to_string_pretty(value)?);
        }
    }
    Ok(())
}

fn render_table(columns: &[String], rows: &[Vec<serde_json::Value>]) {
    let mut widths: Vec<usize> = columns.iter().map(|column| column.len()).collect();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            if let Some(width) = widths.get_mut(index) {
                *width = (*width).max(render_value(value).len());
            }
        }
    }

    print_row(columns.iter().map(String::as_str), &widths);
    print_row(widths.iter().map(|width| "-".repeat(*width)), &widths);
    for row in rows {
        print_row(row.iter().map(render_value), &widths);
    }
}

fn print_row<I, S>(values: I, widths: &[usize])
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    for (index, value) in values.into_iter().enumerate() {
        if index > 0 {
            print!("  ");
        }
        let width = widths.get(index).copied().unwrap_or_default();
        print!("{:<width$}", value.as_ref(), width = width);
    }
    println!();
}

fn render_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => value.to_string(),
    }
}

fn default_socket_path(file_name: &str) -> PathBuf {
    if let Some(runtime_dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(runtime_dir)
            .join("voipswitch")
            .join(file_name);
    }
    PathBuf::from("/tmp/voipswitch").join(file_name)
}
