//! bee-sync client — sends files via parallel chunk connections.
//!
//! Architecture:
//! - Control channel: single TCP/TLS connection for handshake (port 19999)
//! - Data channels: Data ports 45000-46000 allocated per chunk (parallel transfer)
//! - Resume: server tracks received chunks, client queries before sending

use std::{
    fs,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Result, anyhow};
use indicatif::ProgressStyle;
use log::{debug, error, info};

use crate::{
    file_ops::file_hash,
    protocol::{frame, handshake},
};

mod tls;
mod worker;

use tls::{Stream, connect_to_server};
use worker::{WorkerConfig, worker};

/// Default chunk size: 5 MiB
pub const DEFAULT_CHUNK_SIZE: &str = "5M";

/// Default parallel connections
pub const DEFAULT_PARALLEL: usize = 10;

/// Default retries per chunk
pub const DEFAULT_RETRIES: usize = 3;

#[derive(Clone)]
pub struct ClientConfig {
    pub host: String,
    pub port: u16,
    pub filepath: String,
    pub chunk_size: usize,
    pub parallel: usize,
    pub retries: usize,
    pub tls: bool,
    pub tls_no_verify: bool,
    pub verbose: bool,
}

/// Main client entry point
pub async fn run_client(config: ClientConfig) -> i32 {
    // Setup transfer
    let (file_size, num_chunks) = setup_transfer(&config);

    // Perform handshake
    let (mut control_sock, status, data_ports) = match perform_handshake(&config).await {
        Ok(result) => result,
        Err(code) => return code,
    };

    match status {
        handshake::RESP_OK => {
            debug!("Data ports: {:?}", data_ports);
        }
        handshake::RESP_EXISTS => {
            info!("File already exists on server, nothing to transfer");
            return 0;
        }
        _ => {
            error!("Server rejected handshake (status={})", status);
            return 1;
        }
    };

    // Setup workers
    let (worker_assignments, shutdown_flag, progress_bar) =
        setup_workers(&config, num_chunks, &data_ports, file_size);

    // Run workers
    let all_failed = run_workers(
        &config,
        worker_assignments,
        data_ports,
        file_size,
        progress_bar,
        shutdown_flag,
    )
    .await;

    if !all_failed.is_empty() {
        error!("Failed chunks: {:?}", all_failed);
        return 1;
    }

    // Wait for server to confirm assembly
    info!(
        "All {} chunks sent, waiting for server confirmation...",
        num_chunks
    );

    match recv_final_status(&mut control_sock).await {
        Ok(true) => {
            info!("Server confirmed transfer complete");
            0
        }
        Ok(false) => {
            error!("Server reported transfer failure");
            1
        }
        Err(e) => {
            error!("Failed to receive server confirmation: {}", e);
            1
        }
    }
}

/// Build handshake message for given file
///
/// Constructs a handshake frame containing file metadata for server negotiation.
/// The frame format is:
/// - 4 bytes: handshake::MAGIC constant
/// - 2 bytes: filename length (big-endian)
/// - N bytes: filename (UTF-8)
/// - 8 bytes: file size (big-endian)
/// - 4 bytes: chunk size (big-endian)
/// - 4 bytes: number of chunks (big-endian)
/// - 32 bytes: full file BLAKE3 hash
///
/// # Arguments
/// * `filepath` - Path to the file to transfer
/// * `chunk_size` - Desired chunk size in bytes
///
/// # Returns
/// * `Ok(Vec<u8>)` - Complete handshake frame ready to send
/// * `Err(e)` - Error if file operations fail
pub async fn build_handshake(filepath: &str, chunk_size: usize) -> Result<Vec<u8>> {
    let filename = std::path::Path::new(filepath)
        .file_name()
        .ok_or(anyhow!("Invalid filename"))?
        .to_string_lossy()
        .to_string();
    let filename_bytes = filename.into_bytes();
    let file_size = fs::metadata(filepath)
        .map_err(|e| anyhow!("Failed to get file size: {}", e))?
        .len();
    let num_chunks = if file_size > 0 {
        (file_size as usize).div_ceil(chunk_size)
    } else {
        1
    };
    let full_hash = file_hash(filepath)?;

    let mut handshake =
        Vec::with_capacity(handshake::PREFIX_SIZE + filename_bytes.len() + handshake::SUFFIX_SIZE);
    handshake.extend_from_slice(&handshake::MAGIC);
    handshake.extend_from_slice(&(filename_bytes.len() as u16).to_be_bytes());
    handshake.extend_from_slice(&filename_bytes);
    handshake.extend_from_slice(&file_size.to_be_bytes());
    handshake.extend_from_slice(&(chunk_size as u32).to_be_bytes());
    handshake.extend_from_slice(&(num_chunks as u32).to_be_bytes());
    handshake.extend_from_slice(&full_hash);

    Ok(handshake)
}

/// Parse server handshake response
///
/// Parses the server's response to the handshake message.
/// Response format:
/// - 1 byte: status code (0=OK, 1=ERROR, 2=FILE_EXISTS)
/// - 1 byte: number of data ports
/// - N*2 bytes: list of data port numbers (big-endian)
///
/// # Arguments
/// * `data` - Raw response bytes from server
///
/// # Returns
/// * `Ok((status, data_ports))` - Status code and list of available data ports
/// * `Err(e)` - Error if response format is invalid
pub fn parse_response(data: &[u8]) -> Result<(u8, Vec<u16>)> {
    if data.len() < 2 {
        return Err(anyhow!("Response too short"));
    }

    let status = data[0];
    let num_ports = data[1] as usize;

    if data.len() < 2 + num_ports * 2 {
        return Err(anyhow!("Response too short for ports"));
    }

    let mut ports = Vec::with_capacity(num_ports);

    for i in 0..num_ports {
        let offset = 2 + i * 2;
        let port = u16::from_be_bytes([data[offset], data[offset + 1]]);
        ports.push(port);
    }

    Ok((status, ports))
}

/// Setup transfer: validate file, compute size and chunks
///
/// # Arguments
/// * `config` - Client configuration containing file path and chunk size
///
/// # Returns
/// * `(u64, usize)` - Tuple of (file_size, num_chunks)
///
/// # Panics
/// * Exits with code 1 if file not found
fn setup_transfer(config: &ClientConfig) -> (u64, usize) {
    if !std::path::Path::new(&config.filepath).is_file() {
        error!("File not found: {}", config.filepath);
        std::process::exit(1);
    }

    let file_size = std::fs::metadata(&config.filepath)
        .map(|m| m.len())
        .unwrap_or(0);

    let num_chunks = if file_size > 0 {
        (file_size as usize).div_ceil(config.chunk_size)
    } else {
        1
    };

    (file_size, num_chunks)
}

/// Perform handshake with server
///
/// Establishes control connection, sends file metadata, and receives
/// server's response including available data ports for parallel transfer.
///
/// # Arguments
/// * `config` - Client configuration
///
/// # Returns
/// * `Ok((control_sock, status, data_ports))` - Control socket, response status, and data ports
/// * `Err(code)` - Exit code on failure (1)
///
/// # Protocol Flow
/// 1. Connect to server via TLS/TCP
/// 2. Send handshake message with file metadata
/// 3. Receive server response with status and data port list
async fn perform_handshake(config: &ClientConfig) -> Result<(Box<dyn Stream>, u8, Vec<u16>), i32> {
    info!("Connecting to {}:{}...", config.host, config.port);

    let mut control_sock = match connect_to_server(
        config.host.clone(),
        config.port,
        config.tls,
        config.tls_no_verify,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to connect: {}", e);
            return Err(1);
        }
    };

    let handshake_msg = match build_handshake(&config.filepath, config.chunk_size).await {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to build handshake: {}", e);
            return Err(1);
        }
    };

    if let Err(e) = frame::send_timeout(&mut control_sock, &handshake_msg).await {
        error!("Failed to send handshake: {}", e);
        return Err(1);
    }

    let response = match frame::recv_timeout(&mut control_sock).await {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to receive response: {}", e);
            return Err(1);
        }
    };

    let (status, data_ports) = match parse_response(&response) {
        Ok((s, p)) => (s, p),
        Err(e) => {
            error!("Failed to parse response: {}", e);
            return Err(1);
        }
    };

    Ok((control_sock, status, data_ports))
}

/// Receive final transfer status from server after assembly
async fn recv_final_status(stream: &mut (impl tokio::io::AsyncRead + Unpin)) -> Result<bool> {
    let data = frame::recv_timeout(stream).await?;

    if data.is_empty() {
        return Ok(false);
    }

    Ok(data[0] == handshake::RESP_OK)
}

/// Setup workers: distribute chunks, create progress bar, set up signal handling
///
/// Distributes file chunks across worker threads using round-robin assignment
/// to available data ports. Initializes progress tracking and signal handling.
///
/// # Arguments
/// * `config` - Client configuration with parallel and retries settings
/// * `num_chunks` - Total number of chunks to transfer
/// * `data_ports` - List of available data ports from server
/// * `file_size` - Total file size in bytes
///
/// # Returns
/// * `(Vec<Vec<usize>>, Arc<AtomicBool>, Arc<Mutex<ProgressBar>>)` -
///   Worker chunk assignments, shutdown flag, and progress bar
///
/// # Worker Assignment
/// Chunks distributed round-robin: chunk i goes to worker (i % num_workers)
fn setup_workers(
    config: &ClientConfig,
    num_chunks: usize,
    data_ports: &[u16],
    file_size: u64,
) -> (
    Vec<Vec<usize>>,
    Arc<AtomicBool>,
    Arc<Mutex<indicatif::ProgressBar>>,
) {
    // Distribute chunks across workers (round-robin)
    let num_workers = std::cmp::min(config.parallel, data_ports.len());
    let mut worker_assignments: Vec<Vec<usize>> = vec![Vec::new(); num_workers];

    for i in 0..num_chunks {
        let worker_idx = i % num_workers;
        worker_assignments[worker_idx].push(i);
    }

    // Progress tracking
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    // Create progress bar (disabled in debug mode)
    let progress_bar = Arc::new(Mutex::new(indicatif::ProgressBar::new(file_size)));
    {
        let pb = progress_bar.lock().unwrap();
        if !config.verbose {
            pb.set_style(ProgressStyle::default_bar()
                .template("[{bar:40}] {percent:>3}% {bytes:>10}/{total_bytes} {bytes_per_sec} eta {eta}")
                .unwrap());
        }
    }

    // Signal handling
    let shutdown_flag_clone = shutdown_flag.clone();

    ctrlc::set_handler(move || {
        shutdown_flag_clone.store(true, Ordering::SeqCst);
    })
    .ok();

    println!(
        "Sending {} chunks with {} persistent connections...",
        num_chunks, num_workers
    );

    (worker_assignments, shutdown_flag, progress_bar)
}

/// Run workers and collect results
///
/// Spawns worker tasks to transfer assigned chunks in parallel over
/// persistent connections. Collects failure information and finishes
/// progress bar before returning.
///
/// # Arguments
/// * `config` - Client configuration
/// * `worker_assignments` - Chunk indices assigned to each worker
/// * `data_ports` - Available data ports from server
/// * `file_size` - Total file size in bytes
/// * `progress_bar` - Shared progress bar for UI updates
/// * `shutdown_flag` - Atomic flag for graceful shutdown on signal
///
/// # Returns
/// * `Vec<usize>` - List of failed chunk indices
///
/// # Resume Support
/// Workers query server for already-received chunks before sending,
/// enabling transfer resumption from last checkpoint.
async fn run_workers(
    config: &ClientConfig,
    worker_assignments: Vec<Vec<usize>>,
    data_ports: Vec<u16>,
    file_size: u64,
    progress_bar: Arc<Mutex<indicatif::ProgressBar>>,
    shutdown_flag: Arc<AtomicBool>,
) -> Vec<usize> {
    let mut all_failed = Vec::new();
    let chunk_lists: Vec<Vec<usize>> = worker_assignments.into_iter().collect();
    let mut handles = Vec::new();

    for (w_idx, chunk_list) in chunk_lists.iter().enumerate() {
        if chunk_list.is_empty() {
            continue;
        }

        let port_idx = w_idx % data_ports.len();
        let port = data_ports[port_idx];

        let worker_config = WorkerConfig {
            host: config.host.clone(),
            port,
            filepath: config.filepath.clone(),
            chunk_indices: chunk_list.clone(),
            chunk_size: config.chunk_size,
            file_size,
            progress_bar: progress_bar.clone(),
            shutdown_flag: shutdown_flag.clone(),
            retries: config.retries,
        };

        let handle = tokio::spawn(async move { worker(worker_config).await });

        handles.push(handle);
    }

    for handle in handles {
        if let Ok((_ok_list, fail_list)) = handle.await {
            all_failed.extend(fail_list);
        }
    }

    // Finish progress bar (abandon if any chunk failed, so it doesn't lie at 100%)
    {
        let pb = progress_bar.lock().unwrap();

        if all_failed.is_empty() {
            pb.finish();
        } else {
            pb.abandon();
        }
    }

    all_failed
}
