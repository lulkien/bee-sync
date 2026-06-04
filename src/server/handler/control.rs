//! Control connection handler - handles handshake and transfer coordination
//!
//! This module contains the logic for handling control connections from clients,
//! including handshake parsing, receiver setup, and transfer orchestration.

use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, anyhow, bail};
use log::{debug, error, info};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    task::JoinHandle,
};

use crate::{
    file_ops::file_hash,
    protocol::{frame, handshake},
};

use super::{
    super::{
        DATA_PORT_END, DATA_PORT_START, FileReceiver, TOTAL_DATA_PORTS, register_receivers,
        release_ports, remove_receiver, try_reserve_ports,
    },
    handle_data_connection,
};

/// Parsed handshake data from client
///
/// Contains all metadata extracted from the client's handshake frame.
#[derive(Debug)]
pub struct HandshakeData {
    pub filename: String,
    pub safe_name: String,
    pub file_size: u64,
    pub chunk_size: usize,
    pub num_chunks: usize,
    pub full_hash: [u8; 32],
}

/// Handle control connection: parse handshake, allocate data ports, coordinate transfer
///
/// # Arguments
/// - `stream`: Control connection TCP stream (already TLS-handshaked if needed)
/// - `output_dir`: Directory to save received files
/// - `max_parallel`: Maximum parallel data connections
///
/// # Returns
/// - `Ok(())`: Transfer completed successfully
/// - `Err(e)`: Error during handshake or transfer
pub async fn handle_control_connection(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    client_addr: &str,
    output_dir: &str,
    temp_dir: &str,
    max_parallel: usize,
) -> Result<()> {
    // Receive handshake frame
    let data = match frame::recv_timeout(&mut stream).await {
        Ok(d) => d,
        Err(_) => {
            error!("Failed to receive handshake");
            return Ok(());
        }
    };

    if data.is_empty() {
        error!("Empty handshake frame");
        return Ok(());
    }

    // Parse and validate handshake
    let handshake = match parse_handshake(&data) {
        Ok(h) => h,
        Err(e) => {
            error!("Handshake validation failed: {}", e);
            send_handshake_response(&mut stream, handshake::RESP_ERR, &[]).await?;
            return Ok(());
        }
    };

    info!(
        "[{}] Handshake: {} ({} bytes, {} chunks of {} bytes)",
        client_addr, handshake.safe_name, handshake.file_size, handshake.num_chunks, handshake.chunk_size
    );

    // Check if file already exists and matches
    if check_existing_file(output_dir, &handshake.safe_name, &handshake.full_hash)? {
        info!("File {} already exists, skipping", handshake.safe_name);
        send_handshake_response(&mut stream, handshake::RESP_EXISTS, &[]).await?;
        return Ok(());
    }

    // Ensure output and temp directories exist before accepting the transfer
    if let Err(e) = fs::create_dir_all(output_dir) {
        error!("Output directory unavailable: {}", e);
        send_handshake_response(&mut stream, handshake::RESP_ERR, &[]).await?;
        return Ok(());
    }
    if temp_dir != output_dir
        && let Err(e) = fs::create_dir_all(temp_dir)
    {
        error!("Temp directory unavailable: {}", e);
        send_handshake_response(&mut stream, handshake::RESP_ERR, &[]).await?;
        return Ok(());
    }

    // Allocate data ports for parallel transfer
    let data_socks = match allocate_sockets(handshake.num_chunks, max_parallel).await {
        Ok(sockets) => sockets,
        Err(e) => {
            error!("Failed to allocate data ports: {}", e);
            send_handshake_response(&mut stream, handshake::RESP_ERR, &[]).await?;
            return Ok(());
        }
    };

    let ports = sockets_to_ports(&data_socks);

    // Create receiver and register for each data port
    let receiver = create_receiver(
        handshake.safe_name,
        handshake.file_size,
        handshake.chunk_size,
        handshake.num_chunks,
        handshake.full_hash,
        output_dir.to_string(),
        temp_dir.to_string(),
    );

    register_receivers(&data_socks, receiver.clone());

    // Send handshake response with data ports
    send_handshake_response(&mut stream, handshake::RESP_OK, &ports).await?;

    // Load metadata from previous transfer and validate existing .part files
    {
        let mut recv = receiver.lock().unwrap();
        if let Some(meta) = recv.load_or_purge_metadata() {
            for &idx in meta.chunk_hashes.keys() {
                recv.received_chunks[idx] = true;
            }
            recv.metadata = meta;
            let count = recv.received_chunks.iter().filter(|&&c| c).count();
            if count > 0 {
                info!("Resuming transfer: {}/{} chunks already valid", count, recv.num_chunks);
            }
        }
    }

    // Spawn data server tasks
    let data_tasks = spawn_data_servers(data_socks, receiver.clone()).await;

    // Wait for all chunks (or client disconnect)
    wait_for_completion(&receiver, &mut stream).await;

    // Determine outcome: skip assembly if the transfer already failed or client disconnected
    let completed = receiver.lock().unwrap().is_complete();
    let failed = receiver.lock().unwrap().failed;
    let success = if failed {
        error!("Transfer aborted due to I/O failure");
        false
    } else if !completed {
        info!("Client disconnected, skipping assembly");
        false
    } else {
        match assemble_file(&receiver) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to assemble file: {}", e);
                false
            }
        }
    };

    // Send final status to client before cleanup
    let final_status = if success {
        handshake::RESP_OK
    } else {
        handshake::RESP_ERR
    };

    match frame::send_timeout(&mut stream, &[final_status]).await {
        Ok(()) => {}
        Err(e) => {
            // Broken pipe is expected when the client already disconnected
            if e.to_string().contains("Broken pipe") {
                debug!("Client disconnected before final status");
            } else {
                error!("Failed to send final status: {}", e);
            }
        }
    }

    // Cleanup
    cleanup(&ports, data_tasks);

    if success {
        info!(
            "[{}] Transfer complete: {} ({} bytes, {} chunks)",
            client_addr, handshake.filename, handshake.file_size, handshake.num_chunks
        );
    } else {
        error!(
            "[{}] Transfer failed: {} ({} bytes, {} chunks)",
            client_addr, handshake.filename, handshake.file_size, handshake.num_chunks
        );
    }

    Ok(())
}

/// Allocate data sockets for parallel chunk transfer
///
/// Binds TCP listeners to ports in the range 45000-46000.
/// Stops when `count` sockets are allocated or range is exhausted.
///
/// # Arguments
/// - `count`: Number of sockets to allocate
/// - `max_parallel`: Maximum allowed parallel connections
///
/// # Returns
/// - `Ok(sockets)`: Vec of (port, TcpListener) tuples
/// - `Err(msg)`: Error if unable to allocate required sockets
async fn allocate_sockets(
    count: usize,
    max_parallel: usize,
) -> Result<Vec<(u16, TcpListener)>, String> {
    let mut sockets = Vec::new();
    let mut port = DATA_PORT_START;
    let num_socks = count.min(max_parallel);

    // Check global port availability before binding
    if !try_reserve_ports(num_socks) {
        return Err(format!(
            "Not enough data ports available (need {}, have {})",
            num_socks, TOTAL_DATA_PORTS
        ));
    }

    for _ in 0..num_socks {
        while port <= DATA_PORT_END {
            match TcpListener::bind(("0.0.0.0", port)).await {
                Ok(listener) => {
                    sockets.push((port, listener));
                    port += 1;
                    break;
                }
                Err(_) => {
                    port += 1;
                }
            }
        }
    }

    if sockets.len() < num_socks {
        return Err(format!(
            "Failed to allocate data port (range {}-{})",
            DATA_PORT_START, DATA_PORT_END
        ));
    }

    Ok(sockets)
}

/// Extract port numbers from bound sockets
///
/// # Arguments
/// - `sockets`: Vec of (port, TcpListener) tuples
///
/// # Returns
/// - Vec of port numbers in order
fn sockets_to_ports(sockets: &[(u16, TcpListener)]) -> Vec<u16> {
    sockets.iter().map(|(port, _)| *port).collect()
}

/// Parse and validate handshake data from client
///
/// # Arguments
/// - `data`: Raw handshake frame bytes
///
/// # Returns
/// - `Ok(HandshakeData)`: Successfully parsed handshake
/// - `Err(String)`: Validation error with message
pub fn parse_handshake(data: &[u8]) -> Result<HandshakeData> {
    // Validate minimum frame length
    if data.len() < handshake::PREFIX_SIZE + handshake::SUFFIX_SIZE {
        bail!(
            "Handshake too short: {} bytes (minimum {})",
            data.len(),
            handshake::PREFIX_SIZE + handshake::SUFFIX_SIZE
        );
    }

    // Validate magic bytes
    // Handshake layout:
    //   data[0..4]     MAGIC        4-byte protocol identifier ("BESN")
    //   data[4..6]     filename_len u16 big-endian, length of UTF-8 filename
    //   data[6..]      filename     variable-length UTF-8 bytes (safe base name only)
    //   then file metadata follows (see below)
    let magic: [u8; 4] = data[0..4]
        .try_into()
        .map_err(|_| anyhow!("Invalid magic length"))?;

    if magic != handshake::MAGIC {
        bail!("Bad MAGIC: {:?}", magic);
    }

    // Extract filename (safe base name, strips any directory components)
    let filename_len = u16::from_be_bytes([data[4], data[5]]) as usize;

    // Cap filename length to common filesystem limit (255 bytes)
    const MAX_FILENAME_LEN: usize = 255;
    if filename_len > MAX_FILENAME_LEN {
        bail!(
            "filename too long: {} bytes (max {})",
            filename_len,
            MAX_FILENAME_LEN
        );
    }

    if data.len() < handshake::PREFIX_SIZE + filename_len + handshake::SUFFIX_SIZE {
        bail!("Handshake too short for filename");
    }

    let offset = handshake::PREFIX_SIZE;
    let filename = String::from_utf8_lossy(&data[offset..offset + filename_len]).to_string();
    let safe_name = Path::new(&filename)
        .file_name()
        .ok_or_else(|| anyhow!("Invalid filename"))?
        .to_string_lossy()
        .to_string();

    // Extract file metadata
    // Layout after filename (offset = handshake::MAGIC(4) + filename_len(2) + filename):
    //   offset + 0..8   file_size    u64 big-endian, total bytes of the source file
    //   offset + 8..12  chunk_size   u32 big-endian, max bytes per chunk (last may be smaller)
    //   offset + 12..16 num_chunks   u32 big-endian, total number of chunks
    //   offset + 16..48 full_hash    raw 32-byte BLAKE3 hash of the complete source file
    let offset = offset + filename_len;

    let file_size = u64::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]);

    let chunk_size = u32::from_be_bytes([
        data[offset + 8],
        data[offset + 9],
        data[offset + 10],
        data[offset + 11],
    ]) as usize;

    let num_chunks = u32::from_be_bytes([
        data[offset + 12],
        data[offset + 13],
        data[offset + 14],
        data[offset + 15],
    ]) as usize;

    let full_hash: [u8; 32] = data[offset + 16..offset + 48]
        .try_into()
        .map_err(|_| anyhow!("Invalid hash length"))?;

    // Security: validate metadata ranges to prevent OOM / panics downstream
    const MAX_CHUNKS: usize = 1_000_000;
    if chunk_size == 0 {
        bail!("chunk_size must be > 0");
    }
    if num_chunks == 0 {
        bail!("num_chunks must be > 0");
    }
    if num_chunks > MAX_CHUNKS {
        bail!("num_chunks {} exceeds maximum {}", num_chunks, MAX_CHUNKS);
    }

    Ok(HandshakeData {
        filename,
        safe_name,
        file_size,
        chunk_size,
        num_chunks,
        full_hash,
    })
}

/// Check if file already exists and matches expected hash
///
/// # Arguments
/// - `output_dir`: Directory to check for existing file
/// - `safe_name`: Safe filename to check
/// - `expected_hash`: Expected BLAKE3 hash of complete file
///
/// # Returns
/// - `Ok(true)`: File exists and matches
/// - `Ok(false)`: File doesn't exist or doesn't match
/// - `Err(e)`: I/O error checking file
pub fn check_existing_file(
    output_dir: &str,
    safe_name: &str,
    expected_hash: &[u8; 32],
) -> Result<bool> {
    let final_path = format!("{}/{}", output_dir, safe_name);

    if !fs::exists(&final_path)? {
        return Ok(false);
    }

    let existing_hash = file_hash(&final_path)?;

    Ok(existing_hash == *expected_hash)
}

/// Create and initialize FileReceiver for a transfer
///
/// # Arguments
/// - `safe_name`: Safe filename
/// - `file_size`: Total file size
/// - `chunk_size`: Size of each chunk
/// - `num_chunks`: Number of chunks
/// - `full_hash`: Expected BLAKE3 hash of complete file
/// - `parts_dir`: Directory for .part files (may differ from output_dir)
///
/// # Returns
/// - `Arc<Mutex<FileReceiver>>`: Initialized receiver
pub fn create_receiver(
    safe_name: String,
    file_size: u64,
    chunk_size: usize,
    num_chunks: usize,
    full_hash: [u8; 32],
    output_dir: String,
    parts_dir: String,
) -> Arc<Mutex<FileReceiver>> {
    Arc::new(Mutex::new(FileReceiver::new(
        safe_name, file_size, chunk_size, num_chunks, full_hash, output_dir, parts_dir,
    )))
}

/// Spawn data server tasks for each allocated port
///
/// # Arguments
/// - `data_socks`: Vec of (port, TcpListener) tuples
/// - `receiver`: Arc<Mutex<FileReceiver>> for this transfer
///
/// # Returns
/// - `Vec<tokio::task::JoinHandle<()>>`: Task handles for cancellation
pub async fn spawn_data_servers(
    data_socks: Vec<(u16, TcpListener)>,
    receiver: Arc<Mutex<FileReceiver>>,
) -> Vec<JoinHandle<()>> {
    let mut data_tasks = Vec::new();

    for (port, listener) in data_socks {
        let _receiver = receiver.clone();

        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => {
                        if let Err(e) = handle_data_connection(stream, port).await {
                            error!("Error handling data connection on port {}: {}", port, e);
                        }
                    }
                    Err(e) => {
                        error!("Error accepting data connection on port {}: {}", port, e);
                        break;
                    }
                }
            }
        });

        data_tasks.push(handle);
    }

    data_tasks
}

/// Wait for all chunks to be received, or client disconnect.
///
/// Races `is_complete()` polling against the control stream. If the stream
/// yields EOF or an error, the client has disconnected and we stop waiting.
///
/// # Arguments
/// - `receiver`: FileReceiver to monitor
/// - `stream`: Control connection (monitored for disconnect)
pub async fn wait_for_completion(
    receiver: &Arc<Mutex<FileReceiver>>,
    stream: &mut (impl AsyncRead + Unpin),
) {
    use tokio::io::AsyncReadExt;

    loop {
        let should_break = {
            let recv = receiver.lock().unwrap();
            recv.is_complete() || recv.failed
        };

        if should_break {
            break;
        }

        // Race: chunks complete, or client disconnects
        let mut buf = [0u8; 1];
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(500)) => {},
            result = stream.read(&mut buf) => {
                match result {
                    Ok(0) | Err(_) => {
                        info!("Client disconnected during transfer");
                        break;
                    }
                    Ok(_) => {
                        // Unexpected data on control channel — ignore
                        continue;
                    }
                }
            }
        }
    }
}

/// Assemble final file from .part chunks
///
/// # Arguments
/// - `receiver`: FileReceiver containing chunk state
///
/// # Returns
/// - `Ok(result)`: Assembly result (true if successful)
/// - `Err(e)`: Error during assembly
pub fn assemble_file(receiver: &Arc<Mutex<FileReceiver>>) -> Result<bool> {
    receiver.lock().unwrap().assemble()
}

/// Send handshake response to client
///
/// Response format:
/// - 1 byte: status code (handshake::RESP_OK/ERR/EXISTS)
/// - 1 byte: number of data ports
/// - N*2 bytes: port numbers (big-endian)
///
/// # Arguments
/// - `stream`: Control connection stream
/// - `status`: Response status code
/// - `ports`: List of allocated data ports
///
/// # Returns
/// - `Ok(())`: Response sent successfully
/// - `Err(e)`: Error sending response
async fn send_handshake_response(
    stream: &mut (impl AsyncWrite + Unpin),
    status: u8,
    ports: &[u16],
) -> Result<()> {
    let mut response = Vec::with_capacity(2 + ports.len() * 2);

    response.push(status);
    response.push(ports.len() as u8);

    for &port in ports {
        response.extend_from_slice(&port.to_be_bytes());
    }

    frame::send_timeout(stream, &response).await?;

    Ok(())
}

/// Cleanup transfer resources
///
/// Unregisters receivers and drops data task handles.
///
/// # Arguments
/// - `ports`: Vec of data port numbers
/// - `data_tasks`: Vec of data server task handles
pub fn cleanup(ports: &[u16], data_tasks: Vec<JoinHandle<()>>) {
    let count = ports.len();

    for &port in ports {
        remove_receiver(port);
    }

    drop(data_tasks);

    if count > 0 {
        release_ports(count);
    }
}
