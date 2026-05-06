#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use rand::distributions::{Alphanumeric, DistString};
use serde::Serialize;
use std::env;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Mutex;
use tauri::{AppHandle, Manager, State};
use tauri_plugin_log::{RotationStrategy, Target, TargetKind, TimezoneStrategy};

#[derive(Clone, Serialize)]
struct SidecarConfig {
    api_base: String,
    app_token: String,
}

struct SidecarState {
    config: Mutex<Option<SidecarConfig>>,
    process: Mutex<Option<Child>>,
}

impl Default for SidecarState {
    fn default() -> Self {
        Self {
            config: Mutex::new(None),
            process: Mutex::new(None),
        }
    }
}

#[tauri::command]
fn get_sidecar_config(state: State<'_, SidecarState>) -> Option<SidecarConfig> {
    let guard = state.config.lock().ok()?;
    guard.clone()
}

#[tauri::command]
fn get_log_directory() -> String {
    logs_dir_path().display().to_string()
}

#[tauri::command]
fn open_log_directory() -> Result<(), String> {
    let path = logs_dir_path();
    std::fs::create_dir_all(&path).map_err(|error| format!("create log dir failed ({}): {error}", path.display()))?;

    #[cfg(target_os = "windows")]
    {
        Command::new("explorer")
            .arg(&path)
            .spawn()
            .map_err(|error| format!("open log dir failed ({}): {error}", path.display()))?;
        return Ok(());
    }

    #[cfg(target_os = "macos")]
    {
        Command::new("open")
            .arg(&path)
            .spawn()
            .map_err(|error| format!("open log dir failed ({}): {error}", path.display()))?;
        return Ok(());
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        Command::new("xdg-open")
            .arg(&path)
            .spawn()
            .map_err(|error| format!("open log dir failed ({}): {error}", path.display()))?;
        return Ok(());
    }

    #[allow(unreachable_code)]
    Err("opening diagnostics folder is not supported on this platform".to_string())
}

fn logs_dir_path() -> PathBuf {
    if let Ok(app_data) = env::var("APPDATA") {
        return PathBuf::from(app_data).join("NBAPropEngine").join("logs");
    }
    if let Ok(local_app_data) = env::var("LOCALAPPDATA") {
        return PathBuf::from(local_app_data).join("NBAPropEngine").join("logs");
    }
    env::temp_dir().join("NBAPropEngine").join("logs")
}

fn pick_port() -> Result<u16, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|error| format!("bind random port failed: {error}"))?;
    let port = listener
        .local_addr()
        .map_err(|error| format!("resolve bound port failed: {error}"))?
        .port();
    drop(listener);
    Ok(port)
}

fn sidecar_executable_path(app: &AppHandle) -> Result<std::path::PathBuf, String> {
    let resource_dir = app
        .path()
        .resource_dir()
        .map_err(|error| format!("resolve resource dir failed: {error}"))?;
    let entries = std::fs::read_dir(&resource_dir)
        .map_err(|error| format!("read resource dir failed ({}): {error}", resource_dir.display()))?;

    for entry in entries {
        let path = entry
            .map_err(|error| format!("read resource entry failed: {error}"))?
            .path();
        let file_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        if file_name.starts_with("nba-sidecar") && file_name.ends_with(".exe") {
            return Ok(path);
        }
    }

    Err(format!(
        "sidecar binary not found in resource dir: {}",
        resource_dir.display()
    ))
}

fn spawn_sidecar(app: &AppHandle, state: &SidecarState) -> Result<(), String> {
    if cfg!(debug_assertions) {
        return Ok(());
    }

    let port = pick_port()?;
    let app_token = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
    let executable = sidecar_executable_path(app)?;
    let logs_dir = logs_dir_path();
    std::fs::create_dir_all(&logs_dir)
        .map_err(|error| format!("create log dir failed ({}): {error}", logs_dir.display()))?;
    let child = Command::new(&executable)
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--app-token")
        .arg(&app_token)
        .env("LOGS_DIR", logs_dir.display().to_string())
        .spawn()
        .map_err(|error| format!("spawn sidecar failed ({}): {error}", executable.display()))?;

    let config = SidecarConfig {
        api_base: format!("http://127.0.0.1:{port}"),
        app_token,
    };
    if let Ok(mut guard) = state.config.lock() {
        *guard = Some(config);
    }
    if let Ok(mut guard) = state.process.lock() {
        *guard = Some(child);
    }
    Ok(())
}

fn main() {
    let sidecar_state = SidecarState::default();
    let logs_dir = logs_dir_path();

    tauri::Builder::default()
        .plugin(
            tauri_plugin_log::Builder::new()
                .clear_targets()
                .targets([
                    Target::new(TargetKind::Stdout),
                    Target::new(TargetKind::Folder {
                        path: logs_dir,
                        file_name: Some("frontend".to_string()),
                    }),
                ])
                .rotation_strategy(RotationStrategy::KeepSome(5))
                .timezone_strategy(TimezoneStrategy::UseLocal)
                .max_file_size(5 * 1024 * 1024)
                .build(),
        )
        .plugin(tauri_plugin_updater::Builder::new().build())
        .manage(sidecar_state)
        .invoke_handler(tauri::generate_handler![
            get_sidecar_config,
            get_log_directory,
            open_log_directory
        ])
        .setup(|app| {
            let app_handle = app.handle().clone();
            let state = app_handle.state::<SidecarState>();
            spawn_sidecar(&app_handle, &state).map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
