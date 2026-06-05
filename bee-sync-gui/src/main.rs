use std::sync::{Arc, Mutex};

use bee_sync::server::{ServerConfig, run_server};
use slint::ComponentHandle;

slint::include_modules!();

struct AppState {
    log_lines: Vec<String>,
}

impl AppState {
    fn push_log(&mut self, msg: &str) {
        self.log_lines.push(msg.to_string());
        if self.log_lines.len() > 1000 {
            self.log_lines.remove(0);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), slint::PlatformError> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let ui = AppWindow::new()?;
    let state = Arc::new(Mutex::new(AppState {
        log_lines: Vec::new(),
    }));

    // Start/Stop callback
    {
        let ui_handle = ui.as_weak();
        let state_rc = state.clone();

        ui.on_start_stop_server(move || {
            let ui = ui_handle.unwrap();
            let state = state_rc.clone();

            if ui.get_server_running() {
                ui.set_server_running(false);
                ui.set_server_status("Stopping...".into());
                // TODO: send shutdown signal
            } else {
                let bind_addr = ui.get_bind_address().to_string();
                let port_str = ui.get_port().to_string();
                let output_dir = ui.get_output_dir().to_string();
                let cert = ui.get_cert_path().to_string();
                let key = ui.get_key_path().to_string();
                let cert_opt = if cert.is_empty() { None } else { Some(cert) };
                let key_opt = if key.is_empty() { None } else { Some(key) };
                let max_parallel: usize = ui.get_max_parallel().parse().unwrap_or(100);

                let port: u16 = port_str.parse().unwrap_or(19999);

                ui.set_server_running(true);
                ui.set_server_status("Running".into());

                let ui_weak = ui.as_weak();
                tokio::spawn(async move {
                    let config = ServerConfig {
                        bind_host: bind_addr,
                        port,
                        output_dir,
                        temp_dir: String::new(), // uses output_dir
                        certfile: cert_opt,
                        keyfile: key_opt,
                        max_parallel,
                    };

                    {
                        let mut s = state.lock().unwrap();
                        s.push_log(&format!(
                            "Server started on {}:{}",
                            config.bind_host, config.port
                        ));
                    }

                    match run_server(config).await {
                        Ok(_) => {
                            let mut s = state.lock().unwrap();
                            s.push_log("Server stopped");
                        }
                        Err(e) => {
                            let mut s = state.lock().unwrap();
                            s.push_log(&format!("Server error: {}", e));
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
