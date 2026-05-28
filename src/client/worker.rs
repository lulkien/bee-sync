//! Worker module — handles parallel chunk transfer logic.

use std::{
    collections::HashSet,
    fs,
    io::{Read, Seek},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use tokio::net::TcpStream;

use crate::{
    file_ops::calc_hash,
    protocol::{frame, chunk},
};

/// Worker configuration
#[derive(Clone)]
pub struct WorkerConfig {
    pub host: String,
    pub port: u16,
    pub filepath: String,
    pub chunk_indices: Vec<usize>,
    pub chunk_size: usize,
    pub file_size: u64,
    pub progress_bar: Arc<Mutex<indicatif::ProgressBar>>,
    pub shutdown_flag: Arc<AtomicBool>,
    pub retries: usize,
}

/// Send one chunk over persistent connection
pub async fn send_chunk(
    stream: &mut TcpStream,
    filepath: &str,
    chunk_index: usize,
    chunk_offset: u64,
    chunk_size: usize,
    retries: usize,
) -> Result<bool> {
    let mut file = fs::File::open(filepath)?;
    file.seek(std::io::SeekFrom::Start(chunk_offset))?;
    let mut chunk_data = vec![0u8; chunk_size];
    let actual_size = file.read(&mut chunk_data)?;
    chunk_data.truncate(actual_size);

    let chunk_hash = calc_hash(&chunk_data);

    let mut header = vec![0u8; chunk::HEADER_SIZE];
    header[0..4].copy_from_slice(&(chunk_index as u32).to_be_bytes());
    header[4..12].copy_from_slice(&chunk_offset.to_be_bytes());
    header[12..16].copy_from_slice(&(actual_size as u32).to_be_bytes());

    // Total bytes on the wire for this chunk: header + data + hash
    let total_bytes = chunk::HEADER_SIZE + actual_size + chunk::HASH_SIZE;

    for attempt in 0..retries {
        // Each retry gets more time (1x, 2x, 3x the base timeout)
        let timeout = frame::timeout_for_bytes(total_bytes)
            .saturating_mul((attempt + 1) as u32);

        tokio::time::timeout(timeout, frame::send_parts(stream, &[&header, &chunk_data, &chunk_hash]))
            .await
            .map_err(|_| anyhow::anyhow!("send chunk {} timed out after {:?}", chunk_index, timeout))??;

        let ack = tokio::time::timeout(timeout, frame::recv(stream))
            .await
            .map_err(|_| anyhow::anyhow!("ack recv for chunk {} timed out after {:?}", chunk_index, timeout))??;
        if ack.is_empty() {
            if attempt < retries - 1 {
                tokio::time::sleep(Duration::from_millis(200 * (attempt + 1) as u64)).await;
                continue;
            }
            return Err(anyhow::anyhow!("Empty ACK"));
        }

        if ack[0] == chunk::ACK_OK {
            return Ok(true);
        } else {
            if attempt < retries - 1 {
                tokio::time::sleep(Duration::from_millis(200 * (attempt + 1) as u64)).await;
                continue;
            }
            return Err(anyhow::anyhow!("hash mismatch"));
        }
    }

    Ok(false)
}

/// Query server which chunks already received
pub async fn query_received(stream: &mut TcpStream) -> Result<HashSet<usize>> {
    frame::send_timeout(stream, &[chunk::QUERY_MAGIC]).await?;
    let resp = frame::recv_timeout(stream).await?;
    if resp.len() < 4 {
        return Ok(HashSet::new());
    }
    let num = u32::from_be_bytes([resp[0], resp[1], resp[2], resp[3]]) as usize;
    let mut received = HashSet::new();
    for i in 0..num {
        let offset = 4 + i * 4;
        let idx = u32::from_be_bytes([
            resp[offset],
            resp[offset + 1],
            resp[offset + 2],
            resp[offset + 3],
        ]) as usize;
        received.insert(idx);
    }
    Ok(received)
}

/// Worker task: open persistent connection, send multiple chunks, query resume
pub async fn worker(config: WorkerConfig) -> (Vec<usize>, Vec<usize>) {
    let mut ok_list = Vec::new();
    let mut fail_list = Vec::new();

    if config.shutdown_flag.load(Ordering::SeqCst) {
        return (ok_list, fail_list);
    }

    let mut stream = match TcpStream::connect((config.host.as_str(), config.port)).await {
        Ok(s) => s,
        Err(_e) => {
            log::error!("Failed to connect to {}:{}", config.host, config.port);
            for idx in config.chunk_indices {
                fail_list.push(idx);
            }
            return (ok_list, fail_list);
        }
    };

    // Query already-received chunks (resume support)
    match query_received(&mut stream).await {
        Ok(already) => {
            if !already.is_empty() {
                log::info!("Server already has {} chunks, skipping", already.len());
            }

            for idx in config.chunk_indices {
                if config.shutdown_flag.load(Ordering::SeqCst) {
                    break;
                }
                if already.contains(&idx) {
                    continue;
                }

                // Guard against overflow (idx * chunk_size) and underflow (file_size - offset)
                let offset = match (idx as u64).checked_mul(config.chunk_size as u64) {
                    Some(o) if o <= config.file_size => o,
                    _ => {
                        log::error!("Chunk {} offset overflow or out of range", idx);
                        fail_list.push(idx);
                        continue;
                    }
                };
                let remaining = (config.file_size - offset) as usize;
                let actual_size = std::cmp::min(config.chunk_size, remaining);

                match send_chunk(&mut stream, &config.filepath, idx, offset, actual_size, config.retries).await {
                    Ok(true) => {
                        ok_list.push(idx);
                        // Update progress
                        let pb = config.progress_bar.lock().unwrap();
                        pb.inc(actual_size as u64);
                    }
                    Ok(false) | Err(_) => {
                        fail_list.push(idx);
                    }
                }
            }
        }
        Err(e) => {
            log::error!("Query failed: {}", e);
            for idx in config.chunk_indices {
                fail_list.push(idx);
            }
        }
    }

    (ok_list, fail_list)
}
