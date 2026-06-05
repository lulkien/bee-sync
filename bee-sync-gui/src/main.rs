use bee_sync_core::server::{ServerConfig, run_server};
use rfd::FileDialog;
use slint::ComponentHandle;

slint::include_modules!();

#[tokio::main]
async fn main() -> Result<(), slint::PlatformError> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let ui = AppWindow::new()?;

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

    // Start/Stop callback
    {
        let ui_handle = ui.as_weak();

        ui.on_start_stop_server(move || {
            let ui = ui_handle.unwrap();

            if ui.get_server_running() {
                ui.set_server_running(false);
                ui.set_server_status("Stopping...".into());
                // TODO: send shutdown signal
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

                ui.set_server_running(true);
                ui.set_server_status("Running".into());

                let ui_weak = ui.as_weak();
                tokio::spawn(async move {
                    let config = ServerConfig {
                        bind_host: bind_addr,
                        port,
                        output_dir,
                        temp_dir,
                        certfile: cert_opt,
                        keyfile: key_opt,
                        max_parallel: 100,
                    };

                    match run_server(config).await {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("Server error: {}", e);
                        }
                    }

                    let ui = ui_weak.unwrap();
                    ui.set_server_running(false);
                    ui.set_server_status("Stopped".into());
                });
            }
        });
    }

    ui.run()
}
