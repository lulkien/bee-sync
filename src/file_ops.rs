//! File operations module for bee-sync.
//!
//! Provides BLAKE3 hashing for chunk and file integrity verification.

use std::{fs::File, io::Read};

use anyhow::Result;

/// BLAKE3 output size (32 bytes)
pub const HASH_SIZE: usize = 32;

/// Calculate BLAKE3 hash of data
pub fn calc_hash(data: &[u8]) -> [u8; HASH_SIZE] {
    blake3::hash(data).into()
}

/// Calculate BLAKE3 hash of entire file
pub fn file_hash(filepath: &str) -> Result<[u8; HASH_SIZE]> {
    let mut file = File::open(filepath)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(hasher.finalize().into())
}
