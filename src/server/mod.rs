//! bee-sync server — receives files via parallel chunk connections.
//!
//! Architecture:
//! - Control channel: single TCP/TLS connection for handshake (port 19999)
//! - Data channels: Data ports 45000-46000 allocated per chunk (parallel transfer)
//! - Resume: server tracks received chunks, client queries before sending
//!
//! # Overview
//!
//! The server accepts file transfer requests over a control connection, allocates
//! data ports for parallel chunk transfer, and coordinates receiving chunks from
//! multiple concurrent connections.
//!
//! # Components
//!
//! - [`ServerConfig`]: Server configuration struct
//! - [`run_server()`]: Main server entry point
//!
//! # Thread Safety
//!
//! - [`ACTIVE_RECEIVERS`]: Global registry using `LazyLock<Mutex<HashMap<...>>>`
//! - Each transfer uses `Arc<Mutex<FileReceiver>>` for shared state

use std::{
    collections::HashMap,
    fs,
    sync::{Arc, LazyLock, Mutex},
};

use log::{error, info};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;

mod file_receiver;
mod handler;
mod metadata;
mod tls;

use file_receiver::FileReceiver;

/// Server configuration
///
/// # Fields
/// - `bind_host`: Host to bind to (default: 0.0.0.0)
/// - `port`: Control port for handshake (default: 19999)
/// - `output_dir`: Directory to save received files
/// - `certfile`: Optional TLS certificate path for encrypted connections
/// - `keyfile`: Optional TLS private key path
/// - `max_parallel`: Maximum parallel data connections per transfer
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_host: String,
    pub port: u16,
    pub output_dir: String,
    pub temp_dir: String,
    pub certfile: Option<String>,
    pub keyfile: Option<String>,
    pub max_parallel: usize,
}

/// Data port range for parallel chunk transfer
pub const DATA_PORT_START: u16 = 45000;
pub const DATA_PORT_END: u16 = 46000;

/// Maximum parallel connections per transfer
pub const MAX_PARALLEL: usize = 100;

/// Maximum concurrent control connections (prevents connection-flood DoS)
pub const MAX_CONCURRENT_CONNECTIONS: usize = 128;

/// Total available data ports (45000–46000 inclusive)
pub(super) const TOTAL_DATA_PORTS: usize = (DATA_PORT_END - DATA_PORT_START + 1) as usize;

/// Global semaphore tracking available data ports to prevent exhaustion
static PORT_POOL: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(TOTAL_DATA_PORTS));

/// Try to reserve `count` data ports from the global pool.
/// Returns true if reserved, false if insufficient ports are available.
pub(super) fn try_reserve_ports(count: usize) -> bool {
    match PORT_POOL.try_acquire_many(count as u32) {
        Ok(permit) => {
            permit.forget(); // released manually via release_ports
            true
        }
        Err(_) => false,
    }
}

/// Release `count` data ports back to the global pool.
pub(super) fn release_ports(count: usize) {
    PORT_POOL.add_permits(count);
}

/// Global registry mapping data ports to active FileReceivers
///
/// Used to route incoming data connections to the correct transfer.
/// Key: data port number, Value: Arc<Mutex<FileReceiver>> for the transfer
static ACTIVE_RECEIVERS: LazyLock<Mutex<HashMap<u16, Arc<Mutex<FileReceiver>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Register receiver for each allocated data port
///
/// # Arguments
/// - `data_socks`: Vec of (port, TcpListener) tuples
/// - `receiver`: Arc<Mutex<FileReceiver>> to register
fn register_receivers(data_socks: &[(u16, TcpListener)], receiver: Arc<Mutex<FileReceiver>>) {
    let mut receivers = ACTIVE_RECEIVERS.lock().unwrap();
    for (port, _) in data_socks {
        receivers.insert(*port, receiver.clone());
    }
}

/// Get the FileReceiver for a given data port
///
/// # Arguments
/// - `port`: Data port number (45000-46000)
///
/// # Returns
/// - `Some(receiver)` if a transfer is active on this port
/// - `None` if no receiver registered for this port
fn get_receiver(port: u16) -> Option<Arc<Mutex<FileReceiver>>> {
    let receivers = ACTIVE_RECEIVERS.lock().unwrap();
    receivers.get(&port).cloned()
}

/// Remove receiver for a given data port
///
/// # Arguments
/// - `port`: Data port number (45000-46000)
fn remove_receiver(port: u16) {
    let mut receivers = ACTIVE_RECEIVERS.lock().unwrap();
    receivers.remove(&port);
}

/// Run the server with given configuration
///
/// # Overview
/// 1. Creates output directory
/// 2. Loads TLS context if cert/key provided
/// 3. Binds control listener on specified port
/// 4. Accepts control connections and spawns handlers
///
/// # Arguments
/// - `config`: ServerConfig with port, output_dir, TLS options
///
/// # Returns
/// - `Ok(())`: Server terminated (never returns in normal operation)
/// - `Err(e)`: Fatal error starting server
pub async fn run_server(config: ServerConfig) -> anyhow::Result<()> {
    fs::create_dir_all(&config.output_dir)?;

    let tls_ctx = if let (Some(cert), Some(key)) = (&config.certfile, &config.keyfile) {
        info!("TLS enabled with cert: {}, key: {}", cert, key);
        Some(tls::load_tls_context(cert, key)?)
    } else {
        None
    };

    let bind_addr = (config.bind_host.as_str(), config.port);
    let control_listener = TcpListener::bind(bind_addr).await?;

    info!("Listening on {}:{}", config.bind_host, config.port);

    let output_dir = config.output_dir.clone();
    let temp_dir = config.temp_dir.clone();
    let max_parallel = config.max_parallel;
    let conn_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));

    loop {
        match control_listener.accept().await {
            Ok((stream, addr)) => {
                info!("Client connected: {}", addr);
                let addr_str = addr.to_string();
                let _tls_ctx = tls_ctx.clone();
                let output_dir = output_dir.clone();
                let temp_dir = temp_dir.clone();
                let permit = match conn_semaphore.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        error!("Connection limit reached, rejecting {}", addr);
                        continue;
                    }
                };

                tokio::spawn(async move {
                    let _permit = permit; // released on drop
                    let result = if let Some(ref ctx) = _tls_ctx {
                        let acceptor = TlsAcceptor::from(ctx.clone());
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                handler::handle_control_connection(
                                    tls_stream,
                                    &addr_str,
                                    &output_dir,
                                    &temp_dir,
                                    max_parallel,
                                )
                                .await
                            }
                            Err(e) => {
                                error!("TLS handshake failed from {}: {}", addr_str, e);
                                return;
                            }
                        }
                    } else {
                        handler::handle_control_connection(
                            stream,
                            &addr_str,
                            &output_dir,
                            &temp_dir,
                            max_parallel,
                        )
                        .await
                    };

                    if let Err(e) = result {
                        error!("Error handling control connection from {}: {}", addr_str, e);
                    }
                });
            }
            Err(e) => {
                error!("Error accepting connection: {}", e);
            }
        }
    }
}
