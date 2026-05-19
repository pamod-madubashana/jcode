#![cfg_attr(not(unix), allow(dead_code, unused_imports))]

use anyhow::{Context, Result};
#[cfg(unix)]
use serde_json::json;
#[cfg(unix)]
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

const SERVER_START_TIMEOUT: Duration = Duration::from_secs(10);
const SERVER_CONNECT_RETRY_DELAY: Duration = Duration::from_millis(50);
const DESKTOP_SESSION_WORKER_LIMIT: usize = 12;

static DESKTOP_SESSION_WORKER_COUNT: AtomicUsize = AtomicUsize::new(0);

struct DesktopSessionWorkerPermit<'a> {
    counter: &'a AtomicUsize,
}

impl Drop for DesktopSessionWorkerPermit<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

fn try_acquire_desktop_session_worker_slot<'a>(
    counter: &'a AtomicUsize,
    limit: usize,
) -> Result<DesktopSessionWorkerPermit<'a>> {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        if current >= limit {
            anyhow::bail!("desktop session worker limit reached ({limit})");
        }
        match counter.compare_exchange_weak(
            current,
            current + 1,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Ok(DesktopSessionWorkerPermit { counter }),
            Err(next_current) => current = next_current,
        }
    }
}

fn spawn_bounded_desktop_session_worker(
    name: impl Into<String>,
    job: impl FnOnce() + Send + 'static,
) -> Result<()> {
    let name = name.into();
    let permit = try_acquire_desktop_session_worker_slot(
        &DESKTOP_SESSION_WORKER_COUNT,
        DESKTOP_SESSION_WORKER_LIMIT,
    )
    .with_context(|| format!("failed to start {name}"))?;
    std::thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            let _permit = permit;
            job();
        })
        .with_context(|| format!("failed to spawn {name}"))?;
    Ok(())
}

mod events;
mod server_io;
mod terminal;

#[cfg(unix)]
use server_io::{
    DrainOutcome, connect_server_with_retry, connect_server_with_retry_path, drain_session_events,
    ensure_server_running, establish_session_id, read_history_reasoning_effort, read_model_catalog,
    read_model_changed, read_reasoning_effort_changed, subscribe_and_establish_session,
    subscribe_to_server, validate_reload_socket_path, write_json_line,
};
use terminal::{compact_title, launch_first_available_terminal, terminal_candidates};
pub use terminal::{launch_validated_resume_session, validate_resume_session_id};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DesktopModelChoice {
    pub model: String,
    pub provider: Option<String>,
    pub api_method: Option<String>,
    pub detail: Option<String>,
    pub available: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DesktopSessionEvent {
    Status(String),
    SessionStarted {
        session_id: String,
    },
    TextDelta(String),
    TextReplace(String),
    ToolStarted {
        name: String,
    },
    ToolExecuting {
        name: String,
    },
    ToolInput {
        delta: String,
    },
    ToolFinished {
        name: String,
        summary: String,
        is_error: bool,
    },
    ModelChanged {
        model: String,
        provider_name: Option<String>,
        error: Option<String>,
    },
    ModelCatalog {
        current_model: Option<String>,
        provider_name: Option<String>,
        models: Vec<DesktopModelChoice>,
    },
    ModelCatalogError {
        error: String,
    },
    StdinRequest {
        request_id: String,
        prompt: String,
        is_password: bool,
        tool_call_id: String,
    },
    Reloading {
        new_socket: Option<String>,
    },
    Reloaded {
        session_id: String,
    },
    Done,
    Error(String),
}

pub type DesktopSessionEventSender = Sender<DesktopSessionEvent>;

#[derive(Clone, Debug)]
pub struct DesktopSessionHandle {
    command_tx: Sender<DesktopSessionCommand>,
}

impl DesktopSessionHandle {
    pub fn cancel(&self) -> Result<()> {
        self.command_tx
            .send(DesktopSessionCommand::Cancel)
            .context("failed to send cancel to desktop session worker")
    }

    pub fn send_stdin_response(&self, request_id: String, input: String) -> Result<()> {
        self.command_tx
            .send(DesktopSessionCommand::StdinResponse { request_id, input })
            .context("failed to send stdin response to desktop session worker")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DesktopSessionCommand {
    Cancel,
    StdinResponse { request_id: String, input: String },
}

pub fn launch_resume_session(session_id: &str, title: &str) -> Result<()> {
    let title = format!("jcode · {}", compact_title(title));
    let candidates = terminal_candidates(&title, &["--resume", session_id]);
    launch_first_available_terminal(candidates, &format!("jcode --resume {session_id}"))
}

pub fn launch_new_session() -> Result<()> {
    let candidates = terminal_candidates("jcode · new session", &["--fresh-spawn"]);
    launch_first_available_terminal(candidates, "jcode")
}

pub fn send_message_to_session(session_id: &str, _title: &str, message: &str) -> Result<()> {
    validate_resume_session_id(session_id).context("refusing to send to invalid session id")?;
    if message.trim().is_empty() {
        anyhow::bail!("empty draft message");
    }

    let session_id = session_id.to_string();
    let message = message.to_string();
    spawn_bounded_desktop_session_worker("jcode-desktop-workspace-message", move || {
        let (_command_tx, command_rx) = mpsc::channel();
        if let Err(error) =
            run_server_session(Some(&session_id), &message, Vec::new(), None, command_rx)
        {
            crate::desktop_log::error(format_args!(
                "jcode-desktop: workspace server message failed session_id={session_id}: {error:#}"
            ));
        }
    })
    .context("failed to spawn desktop workspace message worker")?;

    Ok(())
}

pub fn spawn_fresh_server_session(
    message: String,
    images: Vec<(String, String)>,
    event_tx: DesktopSessionEventSender,
) -> Result<DesktopSessionHandle> {
    if message.trim().is_empty() && images.is_empty() {
        anyhow::bail!("empty draft message");
    }

    let (command_tx, command_rx) = mpsc::channel();
    let handle = DesktopSessionHandle { command_tx };
    spawn_bounded_desktop_session_worker("jcode-desktop-fresh-session", move || {
        if let Err(error) =
            run_server_session(None, &message, images, Some(event_tx.clone()), command_rx)
        {
            crate::desktop_log::error(format_args!(
                "jcode-desktop: fresh server session failed: {error:#}"
            ));
            send_desktop_event_ref(
                Some(&event_tx),
                DesktopSessionEvent::Error(format!("{error:#}")),
            );
        }
    })
    .context("failed to spawn desktop session worker")?;
    Ok(handle)
}

pub fn spawn_message_to_session(
    session_id: String,
    message: String,
    images: Vec<(String, String)>,
    event_tx: DesktopSessionEventSender,
) -> Result<DesktopSessionHandle> {
    validate_resume_session_id(&session_id).context("refusing to send to invalid session id")?;
    if message.trim().is_empty() && images.is_empty() {
        anyhow::bail!("empty draft message");
    }

    let (command_tx, command_rx) = mpsc::channel();
    let handle = DesktopSessionHandle { command_tx };
    spawn_bounded_desktop_session_worker("jcode-desktop-session-message", move || {
        if let Err(error) = run_server_session(
            Some(&session_id),
            &message,
            images,
            Some(event_tx.clone()),
            command_rx,
        ) {
            crate::desktop_log::error(format_args!(
                "jcode-desktop: server session message failed session_id={session_id}: {error:#}"
            ));
            send_desktop_event_ref(
                Some(&event_tx),
                DesktopSessionEvent::Error(format!("{error:#}")),
            );
        }
    })
    .context("failed to spawn desktop session worker")?;
    Ok(handle)
}

#[cfg(unix)]
pub fn spawn_cycle_model(
    direction: i8,
    target_session_id: Option<String>,
    event_tx: DesktopSessionEventSender,
) -> Result<()> {
    spawn_bounded_desktop_session_worker("jcode-desktop-cycle-model", move || {
            if let Err(error) = cycle_model(
                direction,
                target_session_id.as_deref(),
                Some(event_tx.clone()),
            ) {
                crate::desktop_log::error(format_args!(
                    "jcode-desktop: model cycle failed direction={direction} target_session={}: {error:#}",
                    target_session_id.as_deref().unwrap_or("<current>")
                ));
                send_desktop_event_ref(
                    Some(&event_tx),
                    DesktopSessionEvent::ModelCatalogError {
                        error: format!("{error:#}"),
                    },
                );
            }
        })
        .context("failed to spawn desktop model switch worker")?;
    Ok(())
}

#[cfg(unix)]
pub fn spawn_cycle_reasoning_effort(
    direction: i8,
    target_session_id: Option<String>,
    event_tx: DesktopSessionEventSender,
) -> Result<()> {
    spawn_bounded_desktop_session_worker("jcode-desktop-cycle-effort", move || {
            if let Err(error) = cycle_reasoning_effort(
                direction,
                target_session_id.as_deref(),
                Some(event_tx.clone()),
            ) {
                crate::desktop_log::error(format_args!(
                    "jcode-desktop: reasoning effort cycle failed direction={direction} target_session={}: {error:#}",
                    target_session_id.as_deref().unwrap_or("<current>")
                ));
                send_desktop_event_ref(
                    Some(&event_tx),
                    DesktopSessionEvent::ModelCatalogError {
                        error: format!("{error:#}"),
                    },
                );
            }
        })
        .context("failed to spawn desktop reasoning effort worker")?;
    Ok(())
}

#[cfg(not(unix))]
pub fn spawn_cycle_reasoning_effort(
    _direction: i8,
    _target_session_id: Option<String>,
    event_tx: DesktopSessionEventSender,
) -> Result<()> {
    send_desktop_event_ref(
        Some(&event_tx),
        DesktopSessionEvent::ModelCatalogError {
            error: "desktop reasoning effort switching is not implemented on this platform yet"
                .to_string(),
        },
    );
    Ok(())
}

#[cfg(not(unix))]
pub fn spawn_cycle_model(
    _direction: i8,
    _target_session_id: Option<String>,
    event_tx: DesktopSessionEventSender,
) -> Result<()> {
    send_desktop_event_ref(
        Some(&event_tx),
        DesktopSessionEvent::ModelCatalogError {
            error: "desktop model switching is not implemented on this platform yet".to_string(),
        },
    );
    Ok(())
}

#[cfg(unix)]
pub fn spawn_load_model_catalog(
    target_session_id: Option<String>,
    event_tx: DesktopSessionEventSender,
) -> Result<()> {
    spawn_bounded_desktop_session_worker("jcode-desktop-load-model-catalog", move || {
        if let Err(error) = load_model_catalog(target_session_id.as_deref(), Some(event_tx.clone()))
        {
            crate::desktop_log::error(format_args!(
                "jcode-desktop: model catalog load failed target_session={}: {error:#}",
                target_session_id.as_deref().unwrap_or("<current>")
            ));
            send_desktop_event_ref(
                Some(&event_tx),
                DesktopSessionEvent::ModelCatalogError {
                    error: format!("{error:#}"),
                },
            );
        }
    })
    .context("failed to spawn desktop model catalog worker")?;
    Ok(())
}

#[cfg(not(unix))]
pub fn spawn_load_model_catalog(
    _target_session_id: Option<String>,
    event_tx: DesktopSessionEventSender,
) -> Result<()> {
    send_desktop_event_ref(
        Some(&event_tx),
        DesktopSessionEvent::ModelCatalogError {
            error: "desktop model catalog loading is not implemented on this platform yet"
                .to_string(),
        },
    );
    Ok(())
}

#[cfg(unix)]
pub fn spawn_set_model(
    model: String,
    target_session_id: Option<String>,
    event_tx: DesktopSessionEventSender,
) -> Result<()> {
    spawn_bounded_desktop_session_worker("jcode-desktop-set-model", move || {
        if let Err(error) = set_model(&model, target_session_id.as_deref(), Some(event_tx.clone()))
        {
            crate::desktop_log::error(format_args!(
                "jcode-desktop: set model failed model={} target_session={}: {error:#}",
                crate::desktop_log::truncate_for_log(&model, 256),
                target_session_id.as_deref().unwrap_or("<current>")
            ));
            send_desktop_event_ref(
                Some(&event_tx),
                DesktopSessionEvent::ModelCatalogError {
                    error: format!("{error:#}"),
                },
            );
        }
    })
    .context("failed to spawn desktop set model worker")?;
    Ok(())
}

#[cfg(not(unix))]
pub fn spawn_set_model(
    _model: String,
    _target_session_id: Option<String>,
    event_tx: DesktopSessionEventSender,
) -> Result<()> {
    send_desktop_event_ref(
        Some(&event_tx),
        DesktopSessionEvent::ModelCatalogError {
            error: "desktop model switching is not implemented on this platform yet".to_string(),
        },
    );
    Ok(())
}

#[cfg(unix)]
fn cycle_model(
    direction: i8,
    target_session_id: Option<&str>,
    event_tx: Option<DesktopSessionEventSender>,
) -> Result<()> {
    send_desktop_status(&event_tx, "switching model");
    ensure_server_running()?;
    let stream = connect_server_with_retry(SERVER_START_TIMEOUT)?;
    let mut writer = stream
        .try_clone()
        .context("failed to clone server socket writer")?;
    let mut reader = BufReader::new(stream);
    let mut next_request_id = 1_u64;
    subscribe_and_establish_session(
        &mut reader,
        &mut writer,
        &mut next_request_id,
        target_session_id,
        event_tx.as_ref(),
    )?;
    let request_id = next_request_id;
    write_json_line(
        &mut writer,
        json!({
            "type": "cycle_model",
            "id": request_id,
            "direction": direction,
        }),
    )?;
    read_model_changed(
        &mut reader,
        SERVER_START_TIMEOUT,
        event_tx.as_ref(),
        request_id,
    )
}

#[cfg(unix)]
fn load_model_catalog(
    target_session_id: Option<&str>,
    event_tx: Option<DesktopSessionEventSender>,
) -> Result<()> {
    send_desktop_status(&event_tx, "loading models");
    ensure_server_running()?;
    let stream = connect_server_with_retry(SERVER_START_TIMEOUT)?;
    let mut writer = stream
        .try_clone()
        .context("failed to clone server socket writer")?;
    let mut reader = BufReader::new(stream);
    let mut next_request_id = 1_u64;
    subscribe_and_establish_session(
        &mut reader,
        &mut writer,
        &mut next_request_id,
        target_session_id,
        event_tx.as_ref(),
    )?;
    let request_id = next_request_id;
    write_json_line(
        &mut writer,
        json!({
            "type": "get_model_catalog",
            "id": request_id,
        }),
    )?;
    read_model_catalog(
        &mut reader,
        SERVER_START_TIMEOUT,
        event_tx.as_ref(),
        request_id,
    )
}

#[cfg(unix)]
fn set_model(
    model: &str,
    target_session_id: Option<&str>,
    event_tx: Option<DesktopSessionEventSender>,
) -> Result<()> {
    send_desktop_status(&event_tx, "switching model");
    ensure_server_running()?;
    let stream = connect_server_with_retry(SERVER_START_TIMEOUT)?;
    let mut writer = stream
        .try_clone()
        .context("failed to clone server socket writer")?;
    let mut reader = BufReader::new(stream);
    let mut next_request_id = 1_u64;
    subscribe_and_establish_session(
        &mut reader,
        &mut writer,
        &mut next_request_id,
        target_session_id,
        event_tx.as_ref(),
    )?;
    let request_id = next_request_id;
    write_json_line(
        &mut writer,
        json!({
            "type": "set_model",
            "id": request_id,
            "model": model,
        }),
    )?;
    read_model_changed(
        &mut reader,
        SERVER_START_TIMEOUT,
        event_tx.as_ref(),
        request_id,
    )
}

#[cfg(unix)]
fn cycle_reasoning_effort(
    direction: i8,
    target_session_id: Option<&str>,
    event_tx: Option<DesktopSessionEventSender>,
) -> Result<()> {
    const EFFORTS: [&str; 5] = ["none", "low", "medium", "high", "xhigh"];

    send_desktop_status(&event_tx, "switching reasoning effort");
    ensure_server_running()?;
    let stream = connect_server_with_retry(SERVER_START_TIMEOUT)?;
    let mut writer = stream
        .try_clone()
        .context("failed to clone server socket writer")?;
    let mut reader = BufReader::new(stream);
    let mut next_request_id = 1_u64;
    subscribe_and_establish_session(
        &mut reader,
        &mut writer,
        &mut next_request_id,
        target_session_id,
        event_tx.as_ref(),
    )?;

    let history_request_id = next_request_id;
    write_json_line(
        &mut writer,
        json!({
            "type": "get_history",
            "id": history_request_id,
        }),
    )?;
    next_request_id += 1;
    let current = read_history_reasoning_effort(
        &mut reader,
        SERVER_START_TIMEOUT,
        event_tx.as_ref(),
        history_request_id,
    )?;
    let current_index = current
        .as_deref()
        .and_then(|effort| EFFORTS.iter().position(|candidate| *candidate == effort))
        .unwrap_or(EFFORTS.len() - 1);
    let next_index = if direction > 0 {
        (current_index + 1).min(EFFORTS.len() - 1)
    } else {
        current_index.saturating_sub(1)
    };
    let next_effort = EFFORTS[next_index];

    let request_id = next_request_id;
    write_json_line(
        &mut writer,
        json!({
            "type": "set_reasoning_effort",
            "id": request_id,
            "effort": next_effort,
        }),
    )?;
    read_reasoning_effort_changed(
        &mut reader,
        SERVER_START_TIMEOUT,
        event_tx.as_ref(),
        request_id,
    )
}

#[cfg(unix)]
fn run_server_session(
    target_session_id: Option<&str>,
    message: &str,
    images: Vec<(String, String)>,
    event_tx: Option<DesktopSessionEventSender>,
    command_rx: Receiver<DesktopSessionCommand>,
) -> Result<String> {
    send_desktop_status(&event_tx, "starting shared server");
    ensure_server_running()?;
    send_desktop_status(&event_tx, "connecting to shared server");
    let stream = connect_server_with_retry(SERVER_START_TIMEOUT)?;
    let mut writer = stream
        .try_clone()
        .context("failed to clone server socket writer")?;
    let mut reader = BufReader::new(stream);
    let mut next_request_id = 1_u64;

    let subscribe_request_id = next_request_id;
    subscribe_to_server(&mut writer, subscribe_request_id, target_session_id)?;
    next_request_id += 1;

    let session_id = establish_session_id(
        &mut reader,
        &mut writer,
        &mut next_request_id,
        subscribe_request_id,
        event_tx.as_ref(),
    )?;
    send_desktop_event(
        &event_tx,
        DesktopSessionEvent::SessionStarted {
            session_id: session_id.clone(),
        },
    );

    send_desktop_status(&event_tx, "sending message");
    let message_request_id = next_request_id;
    write_json_line(
        &mut writer,
        json!({
            "type": "message",
            "id": message_request_id,
            "content": message,
            "images": images,
        }),
    )?;
    next_request_id += 1;

    let mut current_socket_path = socket_path();
    loop {
        match drain_session_events(
            reader,
            &mut writer,
            &mut next_request_id,
            event_tx.as_ref(),
            &command_rx,
            message_request_id,
        )? {
            DrainOutcome::Terminal => break,
            DrainOutcome::Disconnected => {
                send_desktop_status(&event_tx, "server disconnected, reconnecting");
            }
            DrainOutcome::Reloading { new_socket } => {
                if let Some(path) = new_socket {
                    current_socket_path = validate_reload_socket_path(&current_socket_path, &path)?;
                }
                send_desktop_status(&event_tx, "server reloading, reconnecting");
            }
        }

        let stream = connect_server_with_retry_path(&current_socket_path, SERVER_START_TIMEOUT)?;
        writer = stream
            .try_clone()
            .context("failed to clone reconnected server socket writer")?;
        reader = BufReader::new(stream);
        let subscribe_request_id = next_request_id;
        subscribe_to_server(&mut writer, subscribe_request_id, Some(&session_id))?;
        next_request_id += 1;
        let reconnected_session_id = establish_session_id(
            &mut reader,
            &mut writer,
            &mut next_request_id,
            subscribe_request_id,
            event_tx.as_ref(),
        )?;
        if reconnected_session_id != session_id {
            anyhow::bail!(
                "jcode server reconnected to unexpected session id: expected {session_id}, got {reconnected_session_id}"
            );
        }
        send_desktop_event(
            &event_tx,
            DesktopSessionEvent::Reloaded {
                session_id: reconnected_session_id,
            },
        );
    }
    Ok(session_id)
}

#[cfg(not(unix))]
fn run_server_session(
    _target_session_id: Option<&str>,
    _message: &str,
    _images: Vec<(String, String)>,
    _event_tx: Option<DesktopSessionEventSender>,
    _command_rx: Receiver<DesktopSessionCommand>,
) -> Result<String> {
    anyhow::bail!("desktop server sessions are not implemented on this platform yet")
}

#[cfg(unix)]
fn send_desktop_status(event_tx: &Option<DesktopSessionEventSender>, status: &str) {
    send_desktop_event(event_tx, DesktopSessionEvent::Status(status.to_string()));
}

fn send_desktop_event(event_tx: &Option<DesktopSessionEventSender>, event: DesktopSessionEvent) {
    send_desktop_event_ref(event_tx.as_ref(), event);
}

pub(super) fn send_desktop_event_ref(
    event_tx: Option<&DesktopSessionEventSender>,
    event: DesktopSessionEvent,
) {
    if let Some(event_tx) = event_tx {
        let event_kind = desktop_session_event_kind(&event);
        if event_tx.send(event).is_err() {
            crate::desktop_log::warn(format_args!(
                "jcode-desktop: failed to deliver backend event {event_kind}, receiver is closed"
            ));
        }
    }
}

fn desktop_session_event_kind(event: &DesktopSessionEvent) -> &'static str {
    match event {
        DesktopSessionEvent::Status(_) => "status",
        DesktopSessionEvent::SessionStarted { .. } => "session_started",
        DesktopSessionEvent::TextDelta(_) => "text_delta",
        DesktopSessionEvent::TextReplace(_) => "text_replace",
        DesktopSessionEvent::ToolStarted { .. } => "tool_started",
        DesktopSessionEvent::ToolExecuting { .. } => "tool_executing",
        DesktopSessionEvent::ToolInput { .. } => "tool_input",
        DesktopSessionEvent::ToolFinished { .. } => "tool_finished",
        DesktopSessionEvent::ModelChanged { .. } => "model_changed",
        DesktopSessionEvent::ModelCatalog { .. } => "model_catalog",
        DesktopSessionEvent::ModelCatalogError { .. } => "model_catalog_error",
        DesktopSessionEvent::StdinRequest { .. } => "stdin_request",
        DesktopSessionEvent::Reloading { .. } => "reloading",
        DesktopSessionEvent::Reloaded { .. } => "reloaded",
        DesktopSessionEvent::Done => "done",
        DesktopSessionEvent::Error(_) => "error",
    }
}

pub(super) fn socket_path() -> PathBuf {
    if let Ok(custom) = std::env::var("JCODE_SOCKET") {
        return PathBuf::from(custom);
    }
    if let Ok(dir) = std::env::var("JCODE_RUNTIME_DIR") {
        return PathBuf::from(dir).join("jcode.sock");
    }
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        return PathBuf::from(dir).join("jcode.sock");
    }
    std::env::temp_dir()
        .join(format!("jcode-{}", runtime_user_discriminator()))
        .join("jcode.sock")
}

#[cfg(unix)]
fn runtime_user_discriminator() -> String {
    unsafe { libc::geteuid() }.to_string()
}

#[cfg(not(unix))]
fn runtime_user_discriminator() -> String {
    std::env::var("USERNAME")
        .or_else(|_| std::env::var("USER"))
        .unwrap_or_else(|_| "user".to_string())
}

#[cfg(test)]
mod tests;
