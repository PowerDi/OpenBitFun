//! Agent-independent stdio launch/supervision, line writes and diagnostic tail.
use std::collections::VecDeque;
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::time::Duration;

use openbitfun_core::service::remote_ssh::workspace_state::get_remote_workspace_manager;
use openbitfun_core::util::errors::{OpenBitFunError, OpenBitFunResult};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{oneshot, Mutex};

// The existing ACP supervisor already owns the cross-platform PID-tree helpers.
use crate::client::manager::{configure_process_group, terminate_child_process_tree};

pub(super) type Reader = Pin<Box<dyn AsyncRead + Send>>;
pub(super) type Writer = Pin<Box<dyn AsyncWrite + Send>>;

pub(super) struct ProcessIo {
    pub writer: Writer,
    pub reader: Reader,
    pub stderr: Reader,
    pub shutdown: oneshot::Sender<()>,
    pub terminated: oneshot::Receiver<()>,
    pub exit: tokio::sync::watch::Receiver<Option<String>>,
}

pub(super) struct ProcessConfig<'a> {
    pub client_id: &'static str,
    pub name: &'static str,
    pub executable: &'a Path,
    pub args: &'a [String],
    pub workspace_path: &'a str,
    pub remote_connection_id: Option<&'a str>,
    /// The driver renders its own command and protocol-specific arguments.
    pub remote_command: &'a str,
}

pub(super) async fn spawn(config: ProcessConfig<'_>) -> OpenBitFunResult<ProcessIo> {
    let ProcessConfig {
        client_id,
        name,
        executable,
        args,
        workspace_path,
        remote_connection_id,
        remote_command,
    } = config;
    let (kill_tx, kill_rx) = oneshot::channel();
    let (terminated_tx, terminated_rx) = oneshot::channel();
    let (exit_tx, exit_rx) = tokio::sync::watch::channel(None);
    let (writer, reader, stderr): (Writer, Reader, Reader) = if let Some(connection) =
        remote_connection_id
    {
        let manager = get_remote_workspace_manager()
            .ok_or_else(|| service_error("Remote workspace manager is not initialized"))?;
        let ssh = manager.get_ssh_manager().await.ok_or_else(|| {
            service_error(format!("SSH manager is unavailable for {name} native"))
        })?;
        let command = remote_command;
        let transport = ssh
            .open_workspace_stdio(connection, &command)
            .await
            .map_err(|e| service_error(format!("{name} SSH launch failed: {e}")))?;
        let (writer, reader, stderr, control, completion) = transport.into_parts();
        tokio::spawn(async move {
            tokio::select! {
                _ = kill_rx => {
                    if let Err(error) = control.kill().await {
                        log::warn!("{name} SSH process termination failed: {error}");
                    }
                    completion.wait().await;
                }
                exit = completion.clone().wait() => {
                    let _ = exit_tx.send(Some(format!("{name} SSH process exited: {:?}", exit.exit_code)));
                }
            }
            let _ = terminated_tx.send(());
        });
        (Box::pin(writer), Box::pin(reader), Box::pin(stderr))
    } else {
        let mut command = openbitfun_core::util::process_manager::create_tokio_command(executable);
        configure_process_group(&mut command);
        command
            .args(args)
            .current_dir(workspace_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|e| service_error(format!("{name} native launch failed: {e}")))?;
        let writer = child
            .stdin
            .take()
            .ok_or_else(|| service_error(format!("{name} stdin unavailable")))?;
        let reader = child
            .stdout
            .take()
            .ok_or_else(|| service_error(format!("{name} stdout unavailable")))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| service_error(format!("{name} stderr unavailable")))?;
        tokio::spawn(async move {
            tokio::select! {
                _ = kill_rx => terminate_child_process_tree(client_id, child).await,
                exit = child.wait() => {
                    let _ = exit_tx.send(Some(format!("{name} process exited: {exit:?}")));
                }
            }
            let _ = terminated_tx.send(());
        });
        (Box::pin(writer), Box::pin(reader), Box::pin(stderr))
    };
    Ok(ProcessIo {
        writer,
        reader,
        stderr,
        shutdown: kill_tx,
        terminated: terminated_rx,
        exit: exit_rx,
    })
}

pub(super) async fn write_json_line(
    writer: &Mutex<Writer>,
    frame: &Value,
    timeout: Duration,
    timeout_message: &str,
) -> OpenBitFunResult<()> {
    let mut bytes = serde_json::to_vec(frame).map_err(|e| service_error(e.to_string()))?;
    bytes.push(b'\n');
    tokio::time::timeout(timeout, async {
        let mut writer = writer.lock().await;
        writer
            .write_all(&bytes)
            .await
            .map_err(|e| service_error(e.to_string()))?;
        writer
            .flush()
            .await
            .map_err(|e| service_error(e.to_string()))
    })
    .await
    .map_err(|_| service_error(timeout_message))?
}

pub(super) async fn collect_stderr(reader: Reader, stderr: &Mutex<VecDeque<String>>) {
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let mut tail = stderr.lock().await;
        // Bound diagnostics, not protocol frames or agent behavior.
        tail.push_back(line.chars().take(2048).collect());
        while tail.len() > 20 {
            tail.pop_front();
        }
    }
}

fn service_error(message: impl Into<String>) -> OpenBitFunError {
    OpenBitFunError::service(message.into())
}
