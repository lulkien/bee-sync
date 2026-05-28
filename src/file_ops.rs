//! File operations module for bee-sync.
//!
//! Provides MD5 calculation, chunk splitting, and file assembly.

use std::{fs::File, io::Read};

use anyhow::Result;

/// Calculate MD5 hash of data
pub fn calc_md5(data: &[u8]) -> [u8; 16] {
    md5::compute(data).into()
}

/// Calculate MD5 hash of file
pub fn file_md5(filepath: &str) -> Result<[u8; 16]> {
    let mut file = File::open(filepath)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;
    Ok(calc_md5(&data))
}
