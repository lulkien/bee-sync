//! Wire protocol for bee-sync file transfer.
//!
//! Every message is a length-prefixed frame: 4-byte big-endian length + payload.
//!
//! ## Message types
//!
//! ### Handshake (client → server, on control connection)
//!   MAGIC(4) + filename_len(2,BE) + filename(UTF-8)
//!   + file_size(8,BE) + chunk_size(4,BE) + num_chunks(4,BE) + full_md5(16 raw)
//!
//! ### Handshake response (server → client)
//!   status(1) + num_ports(1) + ports(num_ports × 2,BE)
//!
//! ### Chunk data (client → server, on data connections)
//!   chunk_index(4,BE) + chunk_offset(8,BE) + chunk_size(4,BE)
//!   + chunk_data + chunk_md5(16 raw)
//!
//! ### Chunk ACK (server → client)
//!   single byte: ACK_OK(0x00) or ACK_MD5_MISMATCH(0x01)
//!
//! ### Resume query (client → server, on data connection)
//!   single byte: QUERY_MAGIC(0x01)
//!
//! ### Resume response (server → client)
//!   count(4,BE) + indices(count × 4,BE)

// ── Frame transport layer ──

/// Length-prefixed frame transport.
///
/// Every wire message is wrapped in a 4-byte big-endian length prefix.
pub mod frame {
    use anyhow::Result;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Size of the length-prefix header (4 bytes, big-endian u32)
    pub const HEADER_SIZE: usize = 4;

    /// Send a length-prefixed frame.
    pub async fn send<T: tokio::io::AsyncWrite + Unpin>(stream: &mut T, data: &[u8]) -> Result<()> {
        let len = data.len() as u32;
        let header = len.to_be_bytes();
        stream.write_all(&header).await?;
        stream.write_all(data).await?;
        stream.flush().await?;
        Ok(())
    }

    /// Send a length-prefixed frame assembled from multiple buffers (single
    /// writev-style header — no per-part framing).
    pub async fn send_parts<T: tokio::io::AsyncWrite + Unpin>(
        stream: &mut T,
        parts: &[&[u8]],
    ) -> Result<()> {
        let total_len: usize = parts.iter().map(|p| p.len()).sum();
        let header = (total_len as u32).to_be_bytes();
        stream.write_all(&header).await?;
        for part in parts {
            stream.write_all(part).await?;
        }
        stream.flush().await?;
        Ok(())
    }

    /// Receive a length-prefixed frame. Returns the payload bytes (empty vec
    /// for a zero-length frame).
    pub async fn recv<T: tokio::io::AsyncRead + Unpin>(stream: &mut T) -> Result<Vec<u8>> {
        let mut header = [0u8; HEADER_SIZE];
        stream.read_exact(&mut header).await?;
        let payload_len = u32::from_be_bytes(header) as usize;
        if payload_len == 0 {
            return Ok(Vec::new());
        }
        let mut payload = vec![0u8; payload_len];
        stream.read_exact(&mut payload).await?;
        Ok(payload)
    }
}

// ── Handshake messages ──

/// Handshake protocol: negotiation between client and server on the control
/// connection before chunk transfer begins.
pub mod handshake {
    /// Protocol magic number — appears at the start of every handshake frame
    pub const MAGIC: [u8; 4] = *b"BESN";

    /// Fixed-size prefix before the variable-length filename:
    ///   MAGIC(4) + filename_len(2,BE)  = 6 bytes
    pub const PREFIX_SIZE: usize = 6;

    /// Fixed-size suffix after the variable-length filename:
    ///   file_size(8,BE) + chunk_size(4,BE) + num_chunks(4,BE) + md5(16) = 32 bytes
    pub const SUFFIX_SIZE: usize = 32;

    /// Handshake response: transfer accepted, data ports follow
    pub const RESP_OK: u8 = 0;

    /// Handshake response: server error
    pub const RESP_ERR: u8 = 1;

    /// Handshake response: file already exists with matching MD5
    pub const RESP_EXISTS: u8 = 2;
}

// ── Chunk transfer messages ──

/// Chunk data transfer: sent by client workers over data connections, with
/// per-chunk MD5 verification and resume support.
pub mod chunk {
    /// Chunk message header size:
    ///   chunk_index(4,BE) + chunk_offset(8,BE) + chunk_size(4,BE) = 16 bytes
    pub const HEADER_SIZE: usize = 16;

    /// Per-chunk MD5 digest size (raw 16 bytes)
    pub const MD5_SIZE: usize = 16;

    /// Chunk ACK: MD5 matched, chunk stored
    pub const ACK_OK: u8 = 0;

    /// Chunk ACK: MD5 mismatch, client should retry
    pub const ACK_MD5_MISMATCH: u8 = 1;

    /// Resume query magic byte — client sends this on a data connection to
    /// ask the server which chunks it already has.
    pub const QUERY_MAGIC: u8 = 0x01;
}
