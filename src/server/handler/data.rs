//! Data connection handler - receives individual chunks
//!
//! This module handles the data connections from clients, receiving chunked
//! file data, verifying BLAKE3 hashes, and writing to .part files.
//!
//! # Architecture
//!
//! - [`handle_data_connection()`]: Main entry point for data connections
//! - [`handle_query()`]: Handle chunk index query from client
//! - [`process_chunk()`]: Process a single chunk (parse, verify, write)
//! - [`verify_and_write_chunk()`]: Verify BLAKE3 hash and write chunk to file
//! - [`send_ack()`]: Send acknowledgment back to client

use std::sync::{Arc, Mutex};

use anyhow::Result;
use log::{debug, error};
use tokio::net::TcpStream;

use crate::{
    file_ops,
    protocol::{frame, chunk},
};

use super::super::{file_receiver::FileReceiver, get_receiver};

/// Handle a data connection: receive chunks, verify BLAKE3 hash, write to .part files, send ACK
///
/// # Protocol
///
/// 1. Receives framed messages from client
/// 2. Handles QUERY_MAGIC (0xFF) to return received chunk indices
/// 3. Parses chunk headers (index, offset, size)
/// 4. Verifies BLAKE3 hash of received chunk data
/// 5. Writes valid chunks to .part files
/// 6. Sends ACK_OK (0x00) or ACK_HASH_MISMATCH (0x01)
///
/// # Arguments
///
/// - `stream`: TCP stream for this data connection
/// - `local_port`: Local port number (used to look up FileReceiver)
///
/// # Returns
///
/// - `Ok(())` on successful transfer completion
/// - `Err(e)` on protocol or I/O errors
///
/// # Overview
///
/// The data connection handler processes incoming chunk data from clients.
/// It maintains a loop that:
/// - Receives framed messages from the client
/// - Handles special query messages (returns list of received chunks)
/// - Parses chunk headers and validates data
/// - Verifies BLAKE3 hashes
/// - Writes valid chunks to .part files
/// - Sends acknowledgments back to the client
pub async fn handle_data_connection(mut stream: TcpStream, local_port: u16) -> Result<()> {
    debug!("Data connection on port {}", local_port);

    let receiver = get_receiver(local_port);

    let receiver = match receiver {
        Some(r) => r,
        None => {
            error!("No receiver for port {}", local_port);
            return Ok(());
        }
    };

    debug!(
        "Receiver found for port {}: {}",
        local_port,
        receiver.lock().unwrap().filename
    );

    // Use a generous timeout for chunk frames (based on max payload at min throughput)
    let recv_timeout = frame::timeout_for_bytes(frame::MAX_PAYLOAD);

    loop {
        let data = match frame::recv_with_timeout(&mut stream, recv_timeout).await {
            Ok(d) => d,
            Err(_) => break,
        };

        if data.is_empty() {
            break;
        }

        // Query: return list of received chunk indices
        if data.len() == 1 && data[0] == chunk::QUERY_MAGIC {
            handle_query(&receiver, &mut stream).await?;
            continue;
        }

        // Process chunk data
        if let Err(e) = process_chunk(&receiver, &mut stream, &data).await {
            error!("Error processing chunk: {}", e);
            continue;
        }
    }

    Ok(())
}

/// Handle chunk index query from client
///
/// Returns a list of all received chunk indices to help with resume support.
///
/// # Arguments
///
/// - `receiver`: FileReceiver for this transfer
/// - `stream`: TCP stream to send response on
///
/// # Returns
///
/// - `Ok(())` on successful response send
/// - `Err(e)` on I/O error
async fn handle_query(receiver: &Arc<Mutex<FileReceiver>>, stream: &mut TcpStream) -> Result<()> {
    let received: Vec<usize> = receiver
        .lock()
        .unwrap()
        .received_chunks
        .iter()
        .enumerate()
        .filter(|(_, c)| **c)
        .map(|(i, _)| i)
        .collect();

    let mut resp = Vec::with_capacity(4 + received.len() * 4);
    resp.extend_from_slice(&(received.len() as u32).to_be_bytes());
    for idx in received {
        resp.extend_from_slice(&(idx as u32).to_be_bytes());
    }

    frame::send_timeout(stream, &resp).await?;
    Ok(())
}

/// Process a single chunk: parse header, verify BLAKE3 hash, write to file
///
/// # Arguments
///
/// - `receiver`: FileReceiver for this transfer
/// - `stream`: TCP stream for sending acknowledgments
/// - `data`: Raw chunk data including header and payload
///
/// # Returns
///
/// - `Ok(())` on successful processing
/// - `Err(e)` on parse or I/O error
///
/// # Protocol
///
/// Chunk format:
/// - Bytes 0-3: Chunk index (u32, big-endian)
/// - Bytes 4-11: Chunk offset (u64, big-endian)
/// - Bytes 12-15: Chunk size (u32, big-endian)
/// - Bytes 16..16+size: Chunk data
/// - Bytes 16+size..16+size+32: Chunk BLAKE3 hash (32 bytes)
async fn process_chunk(
    receiver: &Arc<Mutex<FileReceiver>>,
    stream: &mut TcpStream,
    data: &[u8],
) -> Result<()> {
    // Parse chunk header (need chunk_size before we can validate total length)
    let chunk_index = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let _chunk_offset = u64::from_be_bytes([
        data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
    ]);
    let chunk_size = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;

    // Validate total chunk message length (header + data + hash)
    let expected_len = chunk::HEADER_SIZE + chunk_size + chunk::HASH_SIZE;
    if data.len() < expected_len {
        error!(
            "Chunk message too short: {} bytes (expected at least {})",
            data.len(),
            expected_len
        );
        return Ok(());
    }

    // Validate chunk index against receiver's expected range
    let (expected_chunk_size, is_last) = {
        let recv = receiver.lock().unwrap();
        if chunk_index >= recv.num_chunks {
            error!(
                "Chunk index {} out of range (num_chunks={})",
                chunk_index, recv.num_chunks
            );
            return Ok(());
        }
        let last_idx = recv.num_chunks.saturating_sub(1);
        let is_last = chunk_index == last_idx;
        let expected = if is_last {
            // Last chunk may be smaller than recv.chunk_size
            let remainder =
                recv.file_size as usize - (last_idx * recv.chunk_size);
            if remainder == 0 { recv.chunk_size } else { remainder }
        } else {
            recv.chunk_size
        };
        (expected, is_last)
    };

    if chunk_size != expected_chunk_size {
        error!(
            "Chunk {} size mismatch: got {}, expected {}{}",
            chunk_index,
            chunk_size,
            expected_chunk_size,
            if is_last { " (last chunk)" } else { "" }
        );
        return Ok(());
    }

    // Extract chunk data and hash
    let chunk_data = data[chunk::HEADER_SIZE..chunk::HEADER_SIZE + chunk_size].to_vec();
    let chunk_hash_rcvd: [u8; 32] = data
        [chunk::HEADER_SIZE + chunk_size..chunk::HEADER_SIZE + chunk_size + chunk::HASH_SIZE]
        .try_into()
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid chunk hash length")
        })?;

    // Verify hash and write chunk
    let ack = verify_and_write_chunk(receiver, chunk_index, &chunk_data, &chunk_hash_rcvd)?;

    send_ack(stream, ack).await?;

    Ok(())
}

/// Verify BLAKE3 hash and write chunk to .part file
///
/// # Arguments
///
/// - `receiver`: FileReceiver for this transfer
/// - `chunk_index`: Index of the chunk being processed
/// - `chunk_data`: Raw chunk data
/// - `chunk_hash_rcvd`: Received BLAKE3 hash
///
/// # Returns
///
/// - `chunk::ACK_OK` if hash matches
/// - `chunk::ACK_HASH_MISMATCH` if hash doesn't match
fn verify_and_write_chunk(
    receiver: &Arc<Mutex<FileReceiver>>,
    chunk_index: usize,
    chunk_data: &[u8],
    chunk_hash_rcvd: &[u8; 32],
) -> Result<u8> {
    let chunk_hash_calc = file_ops::calc_hash(chunk_data);

    if chunk_hash_calc == *chunk_hash_rcvd {
        let part_path = receiver.lock().unwrap().part_path(chunk_index);
        std::fs::write(&part_path, chunk_data)?;

        {
            let mut recv = receiver.lock().unwrap();
            recv.received_chunks[chunk_index] = true;
        }

        debug!(
            "Chunk {}/{} OK ({} bytes)",
            chunk_index,
            receiver.lock().unwrap().num_chunks,
            chunk_data.len()
        );
        Ok(chunk::ACK_OK)
    } else {
        error!("Chunk {} hash mismatch", chunk_index);
        Ok(chunk::ACK_HASH_MISMATCH)
    }
}

/// Send acknowledgment back to client
///
/// # Arguments
///
/// - `stream`: TCP stream for this data connection
/// - `ack`: Acknowledgment code (chunk::ACK_OK or chunk::ACK_HASH_MISMATCH)
///
/// # Returns
///
/// - `Ok(())` on successful send
/// - `Err(e)` on I/O error
async fn send_ack(stream: &mut TcpStream, ack: u8) -> Result<()> {
    frame::send_timeout(stream, &[ack]).await
}
