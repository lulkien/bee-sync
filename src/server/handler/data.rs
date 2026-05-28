//! Data connection handler - receives individual chunks
//!
//! This module handles the data connections from clients, receiving chunked
//! file data, verifying MD5 checksums, and writing to .part files.
//!
//! # Architecture
//!
//! - [`handle_data_connection()`]: Main entry point for data connections
//! - [`handle_query()`]: Handle chunk index query from client
//! - [`process_chunk()`]: Process a single chunk (parse, verify, write)
//! - [`verify_and_write_chunk()`]: Verify MD5 and write chunk to file
//! - [`send_ack()`]: Send acknowledgment back to client

use std::sync::{Arc, Mutex};

use anyhow::Result;
use log::{debug, error};
use tokio::net::TcpStream;

use crate::{
    file_ops,
    protocol::{
        ACK_MD5_MISMATCH, ACK_OK, CHUNK_HEADER_SIZE, CHUNK_MD5_SIZE, QUERY_MAGIC, recv_frame,
        send_frame,
    },
};

use super::super::{file_receiver::FileReceiver, get_receiver};

/// Handle a data connection: receive chunks, verify MD5, write to .part files, send ACK
///
/// # Protocol
///
/// 1. Receives framed messages from client
/// 2. Handles QUERY_MAGIC (0xFF) to return received chunk indices
/// 3. Parses chunk headers (index, offset, size)
/// 4. Verifies MD5 of received chunk data
/// 5. Writes valid chunks to .part files
/// 6. Sends ACK_OK (0x00) or ACK_MD5_MISMATCH (0x01)
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
/// - Verifies MD5 checksums
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

    loop {
        let data = match recv_frame(&mut stream).await {
            Ok(d) => d,
            Err(_) => break,
        };

        if data.is_empty() {
            break;
        }

        // Query: return list of received chunk indices
        if data.len() == 1 && data[0] == QUERY_MAGIC {
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

    send_frame(stream, &resp).await?;
    Ok(())
}

/// Process a single chunk: parse header, verify MD5, write to file
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
/// - Bytes 16+size..16+size+16: Chunk MD5 (16 bytes)
async fn process_chunk(
    receiver: &Arc<Mutex<FileReceiver>>,
    stream: &mut TcpStream,
    data: &[u8],
) -> Result<()> {
    // Validate chunk message length
    if data.len() < CHUNK_HEADER_SIZE + CHUNK_MD5_SIZE {
        error!("Chunk message too short: {} bytes", data.len());
        return Ok(());
    }

    // Parse chunk header
    let chunk_index = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let _chunk_offset = u64::from_be_bytes([
        data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
    ]);
    let chunk_size = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;

    // Extract chunk data and MD5
    let chunk_data = data[CHUNK_HEADER_SIZE..CHUNK_HEADER_SIZE + chunk_size].to_vec();
    let chunk_md5_rcvd: [u8; 16] = data
        [CHUNK_HEADER_SIZE + chunk_size..CHUNK_HEADER_SIZE + chunk_size + CHUNK_MD5_SIZE]
        .try_into()
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid chunk MD5 length")
        })?;

    // Verify MD5 and write chunk
    let ack = verify_and_write_chunk(receiver, chunk_index, &chunk_data, &chunk_md5_rcvd)?;

    send_ack(stream, ack).await?;

    Ok(())
}

/// Verify MD5 and write chunk to .part file
///
/// # Arguments
///
/// - `receiver`: FileReceiver for this transfer
/// - `chunk_index`: Index of the chunk being processed
/// - `chunk_data`: Raw chunk data
/// - `chunk_md5_rcvd`: Received MD5 hash
///
/// # Returns
///
/// - `ACK_OK` if MD5 matches
/// - `ACK_MD5_MISMATCH` if MD5 doesn't match
fn verify_and_write_chunk(
    receiver: &Arc<Mutex<FileReceiver>>,
    chunk_index: usize,
    chunk_data: &[u8],
    chunk_md5_rcvd: &[u8; 16],
) -> Result<u8> {
    let chunk_md5_calc = file_ops::calc_md5(chunk_data);

    if chunk_md5_calc == *chunk_md5_rcvd {
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
        Ok(ACK_OK)
    } else {
        error!("Chunk {} MD5 mismatch", chunk_index);
        Ok(ACK_MD5_MISMATCH)
    }
}

/// Send acknowledgment back to client
///
/// # Arguments
///
/// - `stream`: TCP stream for this data connection
/// - `ack`: Acknowledgment code (ACK_OK or ACK_MD5_MISMATCH)
///
/// # Returns
///
/// - `Ok(())` on successful send
/// - `Err(e)` on I/O error
async fn send_ack(stream: &mut TcpStream, ack: u8) -> Result<()> {
    send_frame(stream, &[ack]).await
}
