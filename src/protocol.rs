//! Protocol constants and framing for bee-sync file transfer.
//!
//! Frame format: 4-byte big-endian length prefix followed by payload.
//! Handshake: MAGIC(4) + filename_len(2,BE) + filename(UTF-8) + file_size(8,BE)
//!            + chunk_size(4,BE) + num_chunks(4,BE) + full_md5(16 raw)
//! Chunk message: chunk_index(4,BE) + chunk_offset(8,BE) + chunk_size(4,BE)
//!                + chunk_data + chunk_md5(16 raw)

use anyhow::Result;
use tokio::io::AsyncWriteExt;

/// Protocol magic number
pub const MAGIC: [u8; 4] = *b"PYSN";

/// Query magic byte
pub const QUERY_MAGIC: u8 = 0x01;

/// Frame header format: 4-byte big-endian length
pub const FRAME_HEADER_SIZE: usize = 4;

/// Handshake prefix format: MAGIC(4) + filename_len(2,BE)
pub const HANDSHAKE_PREFIX_SIZE: usize = 6;

/// Handshake suffix format: file_size(8,BE) + chunk_size(4,BE) + num_chunks(4,BE) + md5(16)
pub const HANDSHAKE_SUFFIX_SIZE: usize = 32;

/// Chunk header format: chunk_index(4,BE) + chunk_offset(8,BE) + chunk_size(4,BE)
pub const CHUNK_HEADER_SIZE: usize = 16;

/// Chunk MD5 size
pub const CHUNK_MD5_SIZE: usize = 16;

/// Response status: OK
pub const RESP_STATUS_OK: u8 = 0;

/// Response status: Error
pub const RESP_STATUS_ERR: u8 = 1;

/// Response status: File already exists
pub const RESP_STATUS_EXISTS: u8 = 2;

/// ACK status: OK
pub const ACK_OK: u8 = 0;

/// ACK status: MD5 mismatch
pub const ACK_MD5_MISMATCH: u8 = 1;

/// Send length-prefixed frame over async TCP stream
pub async fn send_frame<T>(stream: &mut T, data: &[u8]) -> Result<()>
where
    T: tokio::io::AsyncWrite + Unpin,
{
    let len = data.len() as u32;
    let header = len.to_be_bytes();
    AsyncWriteExt::write_all(stream, &header).await?;
    AsyncWriteExt::write_all(stream, data).await?;
    AsyncWriteExt::flush(stream).await?;
    Ok(())
}

/// Send length-prefixed frame from multiple buffers (zero-copy)
pub async fn send_frame_parts<T>(stream: &mut T, parts: &[&[u8]]) -> Result<()>
where
    T: tokio::io::AsyncWrite + Unpin,
{
    let total_len: usize = parts.iter().map(|p| p.len()).sum();
    let header = (total_len as u32).to_be_bytes();
    AsyncWriteExt::write_all(stream, &header).await?;
    for part in parts {
        AsyncWriteExt::write_all(stream, part).await?;
    }
    AsyncWriteExt::flush(stream).await?;
    Ok(())
}

/// Receive length-prefixed frame from async TCP stream
pub async fn recv_frame<T>(stream: &mut T) -> Result<Vec<u8>>
where
    T: tokio::io::AsyncRead + Unpin,
{
    let mut header = [0u8; FRAME_HEADER_SIZE];
    tokio::io::AsyncReadExt::read_exact(stream, &mut header).await?;
    let payload_len = u32::from_be_bytes(header) as usize;
    if payload_len == 0 {
        return Ok(Vec::new());
    }
    let mut payload = vec![0u8; payload_len];
    tokio::io::AsyncReadExt::read_exact(stream, &mut payload).await?;
    Ok(payload)
}
