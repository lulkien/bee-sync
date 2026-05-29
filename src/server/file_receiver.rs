//! FileReceiver module for bee-sync server.
//!
//! Tracks state for a single incoming file transfer.

use std::{
    fs::{self, File},
    io::{Read, Write},
    path::Path,
};

use anyhow::Result;

/// FileReceiver tracks state for a single incoming file transfer
#[allow(unused)]
pub struct FileReceiver {
    pub filename: String,
    pub file_size: u64,
    pub chunk_size: usize,
    pub num_chunks: usize,
    pub full_hash: [u8; 32],
    pub output_dir: String,
    /// Where .part files are stored during transfer (defaults to output_dir)
    pub parts_dir: String,
    pub received_chunks: Vec<bool>,
    /// Set to true when a disk write fails — signals the control handler to abort
    pub failed: bool,
}

#[allow(unused)]
impl FileReceiver {
    pub fn new(
        filename: String,
        file_size: u64,
        chunk_size: usize,
        num_chunks: usize,
        full_hash: [u8; 32],
        output_dir: String,
        parts_dir: String,
    ) -> Self {
        let received_chunks = vec![false; num_chunks];
        FileReceiver {
            filename,
            file_size,
            chunk_size,
            num_chunks,
            full_hash,
            output_dir,
            parts_dir,
            received_chunks,
            failed: false,
        }
    }

    pub fn part_path(&self, index: usize) -> String {
        format!("{}/{}.part{}", self.parts_dir, self.filename, index)
    }

    pub fn final_path(&self) -> String {
        format!("{}/{}", self.output_dir, self.filename)
    }

    pub fn is_complete(&self) -> bool {
        self.received_chunks.iter().all(|&c| c)
    }

    pub fn assemble(&self) -> Result<bool> {
        let final_path = Path::new(&self.output_dir).join(&self.filename);
        let part_path =
            |i: usize| Path::new(&self.parts_dir).join(format!("{}.part{}", self.filename, i));

        let mut hasher = blake3::Hasher::new();
        let mut out = File::create(&final_path)?;

        for i in 0..self.num_chunks {
            let part = part_path(i);
            if !part.exists() {
                return Ok(false);
            }

            let mut part_file = File::open(&part)?;
            let mut buffer = [0u8; 65536];
            loop {
                let n = part_file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                out.write_all(&buffer[..n])?;
                hasher.update(&buffer[..n]);
            }
            fs::remove_file(&part)?;
        }

        let actual_hash: [u8; 32] = hasher.finalize().into();
        Ok(actual_hash == self.full_hash)
    }
}
