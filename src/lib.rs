//! bee-sync - fast parallel file transfer with BLAKE3 integrity and verified resume.
//!
//! Architecture:
//! - Control channel: single TCP/TLS connection for handshake (port 19999)
//! - Data channels: Data ports 45000-46000 allocated per chunk (parallel transfer)
//! - Resume: server tracks received chunks, client queries before sending

pub mod cli;
pub mod client;
pub mod file_ops;
pub mod protocol;
pub mod server;
pub mod utils;

// Re-export commonly used types for downstream crates (e.g. bee-sync-gui)
pub use client::{ClientConfig, DEFAULT_CHUNK_SIZE, DEFAULT_PARALLEL, DEFAULT_RETRIES, run_client};
pub use server::{ServerConfig, run_server};
