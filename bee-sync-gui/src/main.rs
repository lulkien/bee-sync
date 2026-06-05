use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bee_sync_core::server::{ServerConfig, TransferEvent, run_server};
use rfd::FileDialog;
use slint::{ComponentHandle, Model, VecModel};
use tokio::sync::mpsc;

slint::include_modules!();

#[tokio::main]
async fn main() -> Result<(), slint::PlatformError> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let ui = AppWindow::new()?;

    // Event channel: server → GUI
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TransferEvent>();

    // Transfer model for the UI list
    let transfers = std::rc::Rc::new(VecModel::<TransferData>::default());
    ui.set_transfers(transfers.clone().into());

    // Spawn event receiver — forwards server events to the transfer model
    {
        let ui_handle = ui.as_weak();
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                let ui = ui_handle.clone();

                // Extract fields before moving into the UI closure
                let client_addr = event.client_addr.clone();
                let filename = event.filename.clone();
                let progress = event.progress();
                let complete = event.complete;

                let _ = slint::invoke_from_event_loop(move || {
                    let Some(ui) = ui.upgrade() else { return };

                    let model = ui.get_transfers();
                    let Some(transfers) = model.as_any().downcast_ref::<VecModel<TransferData>>()
                    else {
                        return;
                    };

                    if complete {
                        let row = (0..transfers.row_count()).position(|i| {
                            transfers
                                .row_data(i)
                                .unwrap()
                                .client_info
                                .as_str()
                                .starts_with(client_addr.as_str())
                        });
                        if let Some(idx) = row {
                            transfers.remove(idx);
                        }
                    } else {
                        let existing = (0..transfers.row_count()).position(|i| {
                            transfers
                                .row_data(i)
                                .unwrap()
                                .client_info
                                .as_str()
                                .starts_with(client_addr.as_str())
                        });
                        if existing.is_none() {
                            transfers.push(TransferData {
                                client_info: format!("{} — {}", client_addr, filename).into(),
                                filename: filename.into(),
                                progress,
                            });
                        } else if let Some(i) = existing {
                            transfers.set_row_data(
                                i,
                                TransferData {
                                    client_info: format!("{} — {}", client_addr, filename).into(),
                                    filename: filename.into(),
                                    progress,
                                },
                            );
                        }
                    }
                });
            }
        });
    }

    // File dialog callbacks
    {
        let ui_handle = ui.as_weak();
        ui.on_browse_output_dir(move || {
            let ui = ui_handle.unwrap();
            if let Some(path) = FileDialog::new().pick_folder() {
                ui.set_output_dir(path.to_string_lossy().to_string().into());
            }
        });
    }
    {
        let ui_handle = ui.as_weak();
        ui.on_browse_temp_dir(move || {
            let ui = ui_handle.unwrap();
            if let Some(path) = FileDialog::new().pick_folder() {
                ui.set_temp_dir(path.to_string_lossy().to_string().into());
            }
        });
    }
    {
        let ui_handle = ui.as_weak();
        ui.on_browse_cert(move || {
            let ui = ui_handle.unwrap();
            if let Some(path) = FileDialog::new()
                .add_filter("Certificates", &["pem", "crt", "cer"])
                .pick_file()
            {
                ui.set_cert_path(path.to_string_lossy().to_string().into());
            }
        });
    }
    {
        let ui_handle = ui.as_weak();
        ui.on_browse_key(move || {
            let ui = ui_handle.unwrap();
            if let Some(path) = FileDialog::new()
                .add_filter("Keys", &["pem", "key"])
                .pick_file()
            {
                ui.set_key_path(path.to_string_lossy().to_string().into());
            }
        });
    }

    // Start/Stop callback — single persistent shutdown flag
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    {
        let ui_handle = ui.as_weak();
        let shutdown = shutdown_flag.clone();

        ui.on_start_stop_server(move || {
            let ui = ui_handle.unwrap();

            if ui.get_server_running() {
                // Stop: set the persistent shutdown flag
                shutdown.store(true, Ordering::SeqCst);
                ui.set_server_running(false);
                ui.set_server_status("Stopping...".into());
            } else {
                let bind_addr = ui.get_bind_address().to_string();
                let port: u16 = ui.get_port().parse().unwrap_or(19999);
                let output_dir = ui.get_output_dir().to_string();
                let cert = ui.get_cert_path().to_string();
                let key = ui.get_key_path().to_string();
                let cert_opt = if cert.is_empty() { None } else { Some(cert) };
                let key_opt = if key.is_empty() { None } else { Some(key) };

                let temp_dir = if ui.get_temp_same_as_output() {
                    output_dir.clone()
                } else {
                    ui.get_temp_dir().to_string()
                };

                // Reset shutdown for new server run
                shutdown.store(false, Ordering::SeqCst);

                ui.set_server_running(true);
                ui.set_server_status("Running".into());

                let ui_weak = ui.as_weak();
                let tx = event_tx.clone();
                let shutdown = shutdown.clone();
                tokio::spawn(async move {
                    let config = ServerConfig {
                        bind_host: bind_addr,
                        port,
                        output_dir,
                        temp_dir,
                        certfile: cert_opt,
                        keyfile: key_opt,
                        max_parallel: 100,
                        shutdown,
                        event_sender: Some(tx),
                    };

                    match run_server(config).await {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("Server error: {}", e);
                        }
                    }

                    // Update UI from the event loop thread
                    let _ = slint::invoke_from_event_loop(move || {
                        let Some(ui) = ui_weak.upgrade() else { return };
                        ui.set_server_running(false);
                        ui.set_server_status("Stopped".into());
                    });
                });
            }
        });
    }

    ui.run()
}
