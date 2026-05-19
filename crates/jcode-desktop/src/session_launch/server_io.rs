#![cfg_attr(not(unix), allow(dead_code, unused_imports))]

use anyhow::{Context, Result};
#[cfg(unix)]
use serde_json::{Value, json};
#[cfg(unix)]
use std::io::{self, BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
#[cfg(unix)]
use std::path::PathBuf;
use std::process::{Command, Stdio};
#[cfg(unix)]
use std::sync::mpsc::Receiver;
#[cfg(unix)]
use std::time::{Duration, Instant};

#[cfg(unix)]
use super::events::{
    desktop_event_from_server_value, history_reasoning_effort_from_server_value,
    model_catalog_event_from_server_value,
};
#[cfg(unix)]
use super::terminal::jcode_bin;
#[cfg(unix)]
use super::{
    DesktopSessionCommand, DesktopSessionEvent, DesktopSessionEventSender,
    SERVER_CONNECT_RETRY_DELAY, SERVER_START_TIMEOUT, send_desktop_event_ref, socket_path,
};

#[cfg(unix)]
pub(super) fn ensure_server_running() -> Result<()> {
    if UnixStream::connect(socket_path()).is_ok() {
        return Ok(());
    }

    Command::new(jcode_bin())
        .arg("serve")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to spawn jcode serve")?;

    connect_server_with_retry(SERVER_START_TIMEOUT).map(|_| ())
}

#[cfg(not(unix))]
pub(super) fn ensure_server_running() -> Result<()> {
    anyhow::bail!("desktop server sessions are not implemented on this platform yet")
}

#[cfg(unix)]
pub(super) fn connect_server_with_retry(timeout: Duration) -> Result<UnixStream> {
    connect_server_with_retry_path(&socket_path(), timeout)
}

#[cfg(unix)]
pub(super) fn connect_server_with_retry_path(
    socket_path: &PathBuf,
    timeout: Duration,
) -> Result<UnixStream> {
    let started = Instant::now();
    let mut last_error = None;
    while started.elapsed() < timeout {
        match UnixStream::connect(socket_path) {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
        std::thread::sleep(SERVER_CONNECT_RETRY_DELAY);
    }

    match last_error {
        Some(error) => Err(error).with_context(|| {
            format!(
                "timed out connecting to jcode server at {}",
                socket_path.display()
            )
        }),
        None => anyhow::bail!("timed out connecting to jcode server"),
    }
}

#[cfg(unix)]
pub(super) fn subscribe_to_server(
    writer: &mut UnixStream,
    id: u64,
    target_session_id: Option<&str>,
) -> Result<()> {
    write_json_line(
        writer,
        json!({
            "type": "subscribe",
            "id": id,
            "target_session_id": target_session_id,
            "client_has_local_history": false,
            "allow_session_takeover": false,
        }),
    )
}

#[cfg(unix)]
pub(super) fn establish_session_id(
    reader: &mut BufReader<UnixStream>,
    writer: &mut UnixStream,
    next_request_id: &mut u64,
    subscribe_request_id: u64,
    event_tx: Option<&DesktopSessionEventSender>,
) -> Result<String> {
    if let Some(session_id) = read_session_id_from_events(
        reader,
        SERVER_START_TIMEOUT,
        event_tx,
        Some(subscribe_request_id),
    )? {
        return Ok(session_id);
    }

    let state_request_id = *next_request_id;
    write_json_line(
        writer,
        json!({
            "type": "state",
            "id": state_request_id,
        }),
    )?;
    *next_request_id += 1;
    read_session_id_from_state(reader, SERVER_START_TIMEOUT, event_tx, state_request_id)
}

#[cfg(unix)]
pub(super) fn subscribe_and_establish_session(
    reader: &mut BufReader<UnixStream>,
    writer: &mut UnixStream,
    next_request_id: &mut u64,
    target_session_id: Option<&str>,
    event_tx: Option<&DesktopSessionEventSender>,
) -> Result<String> {
    let subscribe_request_id = *next_request_id;
    subscribe_to_server(writer, subscribe_request_id, target_session_id)?;
    *next_request_id += 1;
    establish_session_id(
        reader,
        writer,
        next_request_id,
        subscribe_request_id,
        event_tx,
    )
}

#[cfg(unix)]
pub(super) fn read_session_id_from_events(
    reader: &mut BufReader<UnixStream>,
    timeout: Duration,
    event_tx: Option<&DesktopSessionEventSender>,
    complete_request_id: Option<u64>,
) -> Result<Option<String>> {
    reader
        .get_ref()
        .set_read_timeout(Some(SERVER_CONNECT_RETRY_DELAY))
        .context("failed to configure server socket timeout")?;
    let started = Instant::now();
    let mut line = String::new();
    while started.elapsed() < timeout {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => anyhow::bail!("jcode server disconnected before assigning a session"),
            Ok(_) => {
                let value = parse_server_event_line(&line, "waiting for session id")?;
                if value.get("type").and_then(Value::as_str) == Some("session") {
                    let Some(session_id) = value.get("session_id").and_then(Value::as_str) else {
                        anyhow::bail!("jcode server sent malformed session event");
                    };
                    return Ok(Some(session_id.to_string()));
                }
                if let Some(event) = desktop_event_from_server_value(&value) {
                    if !matches!(event, DesktopSessionEvent::Done) {
                        send_desktop_event_ref(event_tx, event);
                    }
                }
                if value.get("type").and_then(Value::as_str) == Some("error") {
                    let message = value
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown server error");
                    crate::desktop_log::error(format_args!(
                        "jcode-desktop: jcode server rejected fresh session: {}",
                        crate::desktop_log::truncate_for_log(message, 2048)
                    ));
                    anyhow::bail!("jcode server rejected fresh session: {message}");
                }
                if value.get("type").and_then(Value::as_str) == Some("done")
                    && complete_request_id
                        .is_some_and(|id| value.get("id").and_then(Value::as_u64) == Some(id))
                {
                    return Ok(None);
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error).context("failed to read jcode server event"),
        }
    }

    anyhow::bail!("timed out waiting for jcode server session id")
}

#[cfg(unix)]
pub(super) fn read_session_id_from_state(
    reader: &mut BufReader<UnixStream>,
    timeout: Duration,
    event_tx: Option<&DesktopSessionEventSender>,
    state_request_id: u64,
) -> Result<String> {
    reader
        .get_ref()
        .set_read_timeout(Some(SERVER_CONNECT_RETRY_DELAY))
        .context("failed to configure server socket timeout")?;
    let started = Instant::now();
    let mut line = String::new();
    while started.elapsed() < timeout {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => anyhow::bail!("jcode server disconnected before returning state"),
            Ok(_) => {
                let value = parse_server_event_line(&line, "waiting for server state")?;
                if value.get("type").and_then(Value::as_str) == Some("state")
                    && value.get("id").and_then(Value::as_u64) == Some(state_request_id)
                {
                    let Some(session_id) = value.get("session_id").and_then(Value::as_str) else {
                        anyhow::bail!("jcode server sent malformed state event");
                    };
                    return Ok(session_id.to_string());
                }
                if let Some(event) = desktop_event_from_server_value(&value) {
                    if !matches!(event, DesktopSessionEvent::Done) {
                        send_desktop_event_ref(event_tx, event);
                    }
                }
                if value.get("type").and_then(Value::as_str) == Some("error")
                    && value.get("id").and_then(Value::as_u64) == Some(state_request_id)
                {
                    let message = value
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown server error");
                    crate::desktop_log::error(format_args!(
                        "jcode-desktop: jcode server rejected state request id={state_request_id}: {}",
                        crate::desktop_log::truncate_for_log(message, 2048)
                    ));
                    anyhow::bail!("jcode server rejected state request: {message}");
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error).context("failed to read jcode server event"),
        }
    }

    anyhow::bail!("timed out waiting for jcode server state")
}

#[cfg(unix)]
pub(super) fn read_model_changed(
    reader: &mut BufReader<UnixStream>,
    timeout: Duration,
    event_tx: Option<&DesktopSessionEventSender>,
    request_id: u64,
) -> Result<()> {
    reader
        .get_ref()
        .set_read_timeout(Some(SERVER_CONNECT_RETRY_DELAY))
        .context("failed to configure server socket timeout")?;
    let started = Instant::now();
    let mut line = String::new();
    while started.elapsed() < timeout {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => anyhow::bail!("jcode server disconnected before switching model"),
            Ok(_) => {
                let value = parse_server_event_line(&line, "waiting for model switch")?;
                if value.get("type").and_then(Value::as_str) == Some("model_changed")
                    && value.get("id").and_then(Value::as_u64) == Some(request_id)
                {
                    if let Some(event) = desktop_event_from_server_value(&value) {
                        send_desktop_event_ref(event_tx, event);
                    }
                    return Ok(());
                }
                if let Some(event) = desktop_event_from_server_value(&value) {
                    if !matches!(event, DesktopSessionEvent::Done) {
                        send_desktop_event_ref(event_tx, event);
                    }
                }
                if value.get("type").and_then(Value::as_str) == Some("error")
                    && value.get("id").and_then(Value::as_u64) == Some(request_id)
                {
                    let message = value
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown server error");
                    crate::desktop_log::error(format_args!(
                        "jcode-desktop: jcode server rejected model switch id={request_id}: {}",
                        crate::desktop_log::truncate_for_log(message, 2048)
                    ));
                    anyhow::bail!("jcode server rejected model switch: {message}");
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error).context("failed to read jcode server event"),
        }
    }

    anyhow::bail!("timed out waiting for jcode server model switch")
}

#[cfg(unix)]
pub(super) fn read_history_reasoning_effort(
    reader: &mut BufReader<UnixStream>,
    timeout: Duration,
    event_tx: Option<&DesktopSessionEventSender>,
    request_id: u64,
) -> Result<Option<String>> {
    reader
        .get_ref()
        .set_read_timeout(Some(SERVER_CONNECT_RETRY_DELAY))
        .context("failed to configure server socket timeout")?;
    let started = Instant::now();
    let mut line = String::new();
    while started.elapsed() < timeout {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => anyhow::bail!("jcode server disconnected before loading history"),
            Ok(_) => {
                let value = parse_server_event_line(&line, "waiting for history")?;
                if value.get("type").and_then(Value::as_str) == Some("history")
                    && value.get("id").and_then(Value::as_u64) == Some(request_id)
                {
                    return Ok(history_reasoning_effort_from_server_value(&value));
                }
                if let Some(event) = desktop_event_from_server_value(&value) {
                    if !matches!(event, DesktopSessionEvent::Done) {
                        send_desktop_event_ref(event_tx, event);
                    }
                }
                if value.get("type").and_then(Value::as_str) == Some("error")
                    && value.get("id").and_then(Value::as_u64) == Some(request_id)
                {
                    let message = value
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown server error");
                    crate::desktop_log::error(format_args!(
                        "jcode-desktop: jcode server rejected history request id={request_id}: {}",
                        crate::desktop_log::truncate_for_log(message, 2048)
                    ));
                    anyhow::bail!("jcode server rejected history request: {message}");
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error).context("failed to read jcode server event"),
        }
    }

    anyhow::bail!("timed out waiting for jcode server history")
}

#[cfg(unix)]
pub(super) fn read_reasoning_effort_changed(
    reader: &mut BufReader<UnixStream>,
    timeout: Duration,
    event_tx: Option<&DesktopSessionEventSender>,
    request_id: u64,
) -> Result<()> {
    reader
        .get_ref()
        .set_read_timeout(Some(SERVER_CONNECT_RETRY_DELAY))
        .context("failed to configure server socket timeout")?;
    let started = Instant::now();
    let mut line = String::new();
    while started.elapsed() < timeout {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => anyhow::bail!("jcode server disconnected before switching reasoning effort"),
            Ok(_) => {
                let value = parse_server_event_line(&line, "waiting for reasoning effort switch")?;
                if value.get("type").and_then(Value::as_str) == Some("reasoning_effort_changed")
                    && value.get("id").and_then(Value::as_u64) == Some(request_id)
                {
                    if let Some(event) = desktop_event_from_server_value(&value) {
                        send_desktop_event_ref(event_tx, event);
                    }
                    return Ok(());
                }
                if let Some(event) = desktop_event_from_server_value(&value) {
                    if !matches!(event, DesktopSessionEvent::Done) {
                        send_desktop_event_ref(event_tx, event);
                    }
                }
                if value.get("type").and_then(Value::as_str) == Some("error")
                    && value.get("id").and_then(Value::as_u64) == Some(request_id)
                {
                    let message = value
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown server error");
                    crate::desktop_log::error(format_args!(
                        "jcode-desktop: jcode server rejected reasoning effort switch id={request_id}: {}",
                        crate::desktop_log::truncate_for_log(message, 2048)
                    ));
                    anyhow::bail!("jcode server rejected reasoning effort switch: {message}");
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error).context("failed to read jcode server event"),
        }
    }

    anyhow::bail!("timed out waiting for jcode server reasoning effort switch")
}

#[cfg(unix)]
pub(super) fn read_model_catalog(
    reader: &mut BufReader<UnixStream>,
    timeout: Duration,
    event_tx: Option<&DesktopSessionEventSender>,
    request_id: u64,
) -> Result<()> {
    reader
        .get_ref()
        .set_read_timeout(Some(SERVER_CONNECT_RETRY_DELAY))
        .context("failed to configure server socket timeout")?;
    let started = Instant::now();
    let mut line = String::new();
    while started.elapsed() < timeout {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => anyhow::bail!("jcode server disconnected before loading model catalog"),
            Ok(_) => {
                let value = parse_server_event_line(&line, "waiting for model catalog")?;
                if value.get("type").and_then(Value::as_str) == Some("history")
                    && value.get("id").and_then(Value::as_u64) == Some(request_id)
                {
                    if let Some(event) = model_catalog_event_from_server_value(&value) {
                        send_desktop_event_ref(event_tx, event);
                        return Ok(());
                    }
                    anyhow::bail!("jcode server returned malformed model catalog");
                }
                if let Some(event) = desktop_event_from_server_value(&value) {
                    if !matches!(event, DesktopSessionEvent::Done) {
                        send_desktop_event_ref(event_tx, event);
                    }
                }
                if value.get("type").and_then(Value::as_str) == Some("error")
                    && value.get("id").and_then(Value::as_u64) == Some(request_id)
                {
                    let message = value
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown server error");
                    crate::desktop_log::error(format_args!(
                        "jcode-desktop: jcode server rejected model catalog request id={request_id}: {}",
                        crate::desktop_log::truncate_for_log(message, 2048)
                    ));
                    anyhow::bail!("jcode server rejected model catalog request: {message}");
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error).context("failed to read jcode server event"),
        }
    }

    anyhow::bail!("timed out waiting for jcode server model catalog")
}

#[cfg(unix)]
pub(super) fn write_json_line(writer: &mut UnixStream, value: Value) -> Result<()> {
    serde_json::to_writer(&mut *writer, &value).context("failed to encode server request")?;
    writer
        .write_all(b"\n")
        .context("failed to send server request")?;
    writer.flush().context("failed to flush server request")
}

#[cfg(unix)]
pub(super) enum DrainOutcome {
    Terminal,
    Disconnected,
    Reloading { new_socket: Option<String> },
}

#[cfg(unix)]
pub(super) fn drain_session_events(
    mut reader: BufReader<UnixStream>,
    writer: &mut UnixStream,
    next_request_id: &mut u64,
    event_tx: Option<&DesktopSessionEventSender>,
    command_rx: &Receiver<DesktopSessionCommand>,
    terminal_request_id: u64,
) -> Result<DrainOutcome> {
    reader
        .get_ref()
        .set_read_timeout(Some(SERVER_CONNECT_RETRY_DELAY))
        .context("failed to configure server socket timeout")?;
    let mut line = String::new();
    loop {
        drain_worker_commands(writer, next_request_id, event_tx, command_rx)?;
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => return Ok(DrainOutcome::Disconnected),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error).context("failed to read jcode server event"),
            Ok(_) => {
                if let Ok(value) = parse_server_event_line(&line, "draining session events") {
                    if value.get("type").and_then(Value::as_str) == Some("reloading") {
                        let new_socket = value
                            .get("new_socket")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned);
                        send_desktop_event_ref(
                            event_tx,
                            DesktopSessionEvent::Reloading {
                                new_socket: new_socket.clone(),
                            },
                        );
                        return Ok(DrainOutcome::Reloading { new_socket });
                    }
                    let is_terminal = match value.get("type").and_then(Value::as_str) {
                        Some("done") => {
                            value.get("id").and_then(Value::as_u64) == Some(terminal_request_id)
                        }
                        Some("error") => value
                            .get("id")
                            .and_then(Value::as_u64)
                            .is_none_or(|id| id == terminal_request_id),
                        _ => false,
                    };
                    if let Some(event) = desktop_event_from_server_value(&value) {
                        if !matches!(event, DesktopSessionEvent::Done) || is_terminal {
                            send_desktop_event_ref(event_tx, event);
                        }
                    }
                    if is_terminal {
                        return Ok(DrainOutcome::Terminal);
                    }
                }
            }
        }
    }
}

#[cfg(unix)]
fn parse_server_event_line(line: &str, context: &str) -> Result<Value> {
    match serde_json::from_str::<Value>(line.trim()) {
        Ok(value) => Ok(value),
        Err(error) => {
            crate::desktop_log::error(format_args!(
                "jcode-desktop: failed to parse jcode server event while {context}: {error}; line={}",
                crate::desktop_log::truncate_for_log(line.trim(), 512)
            ));
            Err(error).context("failed to parse jcode server event")
        }
    }
}

#[cfg(unix)]
pub(super) fn drain_worker_commands(
    writer: &mut UnixStream,
    next_request_id: &mut u64,
    event_tx: Option<&DesktopSessionEventSender>,
    command_rx: &Receiver<DesktopSessionCommand>,
) -> Result<()> {
    while let Ok(command) = command_rx.try_recv() {
        match command {
            DesktopSessionCommand::Cancel => {
                send_desktop_event_ref(
                    event_tx,
                    DesktopSessionEvent::Status("cancelling".to_string()),
                );
                write_json_line(
                    writer,
                    json!({
                        "type": "cancel",
                        "id": *next_request_id,
                    }),
                )?;
                *next_request_id += 1;
            }
            DesktopSessionCommand::StdinResponse { request_id, input } => {
                send_desktop_event_ref(
                    event_tx,
                    DesktopSessionEvent::Status("sending interactive input".to_string()),
                );
                write_json_line(
                    writer,
                    json!({
                        "type": "stdin_response",
                        "id": *next_request_id,
                        "request_id": request_id,
                        "input": input,
                    }),
                )?;
                *next_request_id += 1;
            }
        }
    }
    Ok(())
}
