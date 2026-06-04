//! FileReceiver module for bee-sync server.
//!
//! Tracks state for a single incoming file transfer, including per-chunk
//! BLAKE3 hashes stored in a `.bee-meta` file for safe resume.

use std::{
    fs::{self, File},
    io::{Read, Write},
    path::Path,
};

use anyhow::Result;

use super::metadata::TransferMetadata;

/// FileReceiver tracks state for a single incoming file transfer
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
    /// Per-chunk BLAKE3 hashes persisted to disk for safe resume
    pub metadata: TransferMetadata,
}

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
        let metadata = TransferMetadata::new(chunk_size, num_chunks, file_size, full_hash);
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
            metadata,
        }
    }

    pub fn part_path(&self, index: usize) -> String {
        format!("{}/{}.part{}", self.parts_dir, self.filename, index)
    }

    #[allow(dead_code)]
    pub fn final_path(&self) -> String {
        format!("{}/{}", self.output_dir, self.filename)
    }

    pub fn is_complete(&self) -> bool {
        self.received_chunks.iter().all(|&c| c)
    }

    /// Mark a chunk as received and persist its hash to metadata.
    /// Call this after successfully writing the .part file.
    pub fn record_chunk(&mut self, index: usize, hash: [u8; 32]) -> Result<()> {
        self.received_chunks[index] = true;
        self.metadata.add_chunk(index, hash);
        self.metadata.save(&self.parts_dir, &self.filename)
    }

    /// Load metadata from a previous transfer and validate existing .part files.
    ///
    /// Returns the metadata if valid, or `None` if the transfer parameters
    /// don't match or any .part file is corrupt. Stale files are purged.
    pub fn load_or_purge_metadata(&self) -> Option<TransferMetadata> {
        let meta = match TransferMetadata::load(&self.parts_dir, &self.filename) {
            Ok(Some(m)) => m,
            Ok(None) => return None,   // no previous transfer
            Err(e) => {
                log::warn!("Failed to load metadata: {}, purging", e);
                TransferMetadata::purge(&self.parts_dir, &self.filename, self.num_chunks);
                return None;
            }
        };

        // Validate transfer parameters
        if !meta.params_match(
            self.chunk_size,
            self.num_chunks,
            self.file_size,
            &self.full_hash,
        ) {
            log::info!(
                "Transfer parameters changed (chunk_size={}→{}, num_chunks={}→{}), purging old parts",
                meta.chunk_size, self.chunk_size,
                meta.num_chunks, self.num_chunks,
            );
            TransferMetadata::purge(&self.parts_dir, &self.filename, self.num_chunks);
            return None;
        }

        // Verify every claimed .part file against its stored hash
        let mut valid_indices: Vec<usize> = Vec::new();
        let mut corrupt_count = 0;

        for &idx in meta.chunk_hashes.keys() {
            if meta.verify_part(idx, &self.parts_dir, &self.filename) {
                valid_indices.push(idx);
            } else {
                // Remove the corrupt .part file
                let part_path = self.part_path(idx);
                let _ = fs::remove_file(&part_path);
                corrupt_count += 1;
            }
        }

        if corrupt_count > 0 {
            log::warn!(
                "{} of {} existing chunks corrupt, purged",
                corrupt_count,
                meta.chunk_hashes.len()
            );

            if valid_indices.is_empty() {
                // All chunks corrupt → purge everything
                TransferMetadata::purge(&self.parts_dir, &self.filename, self.num_chunks);
                return None;
            }

            // Partial corruption → rewrite metadata with only valid entries
            let mut clean_meta = TransferMetadata::new(
                self.chunk_size,
                self.num_chunks,
                self.file_size,
                self.full_hash,
            );

            for &idx in &valid_indices {
                clean_meta.add_chunk(idx, meta.chunk_hashes[&idx]);
            }

            if let Err(e) = clean_meta.save(&self.parts_dir, &self.filename) {
                log::error!("Failed to save cleaned metadata: {}", e);
            }

            return Some(clean_meta);
        }

        Some(meta)
    }

    /// Remove all .part files and metadata for this transfer.
    #[allow(dead_code)]
    pub fn purge_parts(&self) {
        for i in 0..self.num_chunks {
            let _ = fs::remove_file(self.part_path(i));
        }
        let _ = fs::remove_file(TransferMetadata::meta_path(&self.parts_dir, &self.filename));
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

        // Clean up metadata on successful assembly
        if let Err(e) = fs::remove_file(TransferMetadata::meta_path(&self.parts_dir, &self.filename)) {
            log::warn!("Failed to remove metadata file: {}", e);
        }

        let actual_hash: [u8; 32] = hasher.finalize().into();
        Ok(actual_hash == self.full_hash)
    }
}
