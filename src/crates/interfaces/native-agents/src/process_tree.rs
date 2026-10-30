//! Process-group launch and process-tree termination for agent CLIs.
#[cfg(unix)]
use std::time::Duration;

use log::warn;
use tokio::process::{Child, Command};

pub fn configure_process_group(command: &mut Command) {
    #[cfg(unix)]
    {
        command.process_group(0);
    }
    #[cfg(not(unix))]
    {
        let _ = command;
    }
}

pub async fn terminate_child_process_tree(client_id: &str, mut child: Child) {
    let pid = child.id();

    #[cfg(unix)]
    if let Some(pid) = pid {
        let process_group = format!("-{}", pid);
        match openbitfun_core::util::process_manager::create_tokio_command("kill")
            .arg("-TERM")
            .arg(&process_group)
            .status()
            .await
        {
            Ok(status) if status.success() => {}
            Ok(status) => {
                warn!(
                    "ACP client process group terminate exited unsuccessfully: id={} pid={} status={}",
                    client_id, pid, status
                );
            }
            Err(error) => {
                warn!(
                    "Failed to terminate ACP client process group: id={} pid={} error={}",
                    client_id, pid, error
                );
            }
        }

        match tokio::time::timeout(Duration::from_millis(750), child.wait()).await {
            Ok(Ok(_)) => return,
            Ok(Err(error)) => {
                warn!(
                    "Failed to wait for ACP client process after terminate: id={} pid={} error={}",
                    client_id, pid, error
                );
            }
            Err(_) => {}
        }

        if let Err(error) = openbitfun_core::util::process_manager::create_tokio_command("kill")
            .arg("-KILL")
            .arg(&process_group)
            .status()
            .await
        {
            warn!(
                "Failed to kill ACP client process group: id={} pid={} error={}",
                client_id, pid, error
            );
        }
        let _ = child.wait().await;
        return;
    }

    #[cfg(windows)]
    if let Some(pid) = pid {
        match openbitfun_core::util::process_manager::create_tokio_command("taskkill")
            .arg("/PID")
            .arg(pid.to_string())
            .arg("/T")
            .arg("/F")
            .status()
            .await
        {
            Ok(status) if status.success() => {
                let _ = child.wait().await;
                return;
            }
            Ok(status) => {
                warn!(
                    "ACP client process tree kill exited unsuccessfully: id={} pid={} status={}",
                    client_id, pid, status
                );
            }
            Err(error) => {
                warn!(
                    "Failed to kill ACP client process tree: id={} pid={} error={}",
                    client_id, pid, error
                );
            }
        }
    }

    if let Err(error) = child.start_kill() {
        warn!(
            "Failed to kill ACP client process: id={} error={}",
            client_id, error
        );
    }
    let _ = child.wait().await;
}
