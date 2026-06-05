//! Transfer metadata — persists chunk hashes and transfer parameters to a
//! `.bee-meta` file alongside `.part` files. Enables safe resume: on reconnect,
//! the server validates that transfer parameters match and re-verifies every
//! existing `.part` against its stored BLAKE3 hash.
//!
//! ## Binary format
//!
//! ```text
//! Offset  Size  Field
//! 0       4     Magic "BEMT"
//! 4       1     Version (1)
//! 5       4     chunk_size   u32 big-endian
//! 9       4     num_chunks   u32 big-endian
//! 13      8     file_size    u64 big-endian
//! 21      32    full_hash    raw 32-byte BLAKE3
//! 53      4     entry_count  u32 big-endian
//! 57      N*36  entries      chunk_index(u32 BE) + chunk_hash(32 bytes)
//! ```

use std::{collections::HashMap, fs, path::Path};

use anyhow::{Result, bail};

/// Magic bytes identifying a bee-sync metadata file
const MAGIC: [u8; 4] = *b"BEMT";

/// Current metadata format version
const VERSION: u8 = 1;

/// Size of the fixed header (before entries)
const HEADER_SIZE: usize = 4 + 1 + 4 + 4 + 8 + 32 + 4; // 57 bytes

/// Size of one chunk entry: index(4) + hash(32)
const ENTRY_SIZE: usize = 36;

/// Transfer metadata persisted alongside `.part` files.
#[derive(Debug, Clone)]
pub struct TransferMetadata {
    pub chunk_size: usize,
    pub num_chunks: usize,
    pub file_size: u64,
    pub full_hash: [u8; 32],
    /// Map of chunk index → BLAKE3 hash of that chunk's data
    pub chunk_hashes: HashMap<usize, [u8; 32]>,
}

impl TransferMetadata {
    /// Create a new empty metadata record for a transfer.
    pub fn new(chunk_size: usize, num_chunks: usize, file_size: u64, full_hash: [u8; 32]) -> Self {
        Self {
            chunk_size,
            num_chunks,
            file_size,
            full_hash,
            chunk_hashes: HashMap::new(),
        }
    }

    /// Record a successfully received chunk.
    pub fn add_chunk(&mut self, index: usize, hash: [u8; 32]) {
        self.chunk_hashes.insert(index, hash);
    }

    /// Path to the metadata file for a given filename in a directory.
    pub fn meta_path(parts_dir: &str, filename: &str) -> String {
        format!("{}/{}.bee-meta", parts_dir, filename)
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.chunk_hashes.len() * ENTRY_SIZE);

        buf.extend_from_slice(&MAGIC);
        buf.push(VERSION);
        buf.extend_from_slice(&(self.chunk_size as u32).to_be_bytes());
        buf.extend_from_slice(&(self.num_chunks as u32).to_be_bytes());
        buf.extend_from_slice(&self.file_size.to_be_bytes());
        buf.extend_from_slice(&self.full_hash);
        buf.extend_from_slice(&(self.chunk_hashes.len() as u32).to_be_bytes());

        // Sort entries by index for deterministic output
        let mut entries: Vec<_> = self.chunk_hashes.iter().collect();
        entries.sort_by_key(|(idx, _)| *idx);

        for (idx, hash) in entries {
            buf.extend_from_slice(&(*idx as u32).to_be_bytes());
            buf.extend_from_slice(hash);
        }

        buf
    }

    /// Deserialize from bytes. Returns an error if the format is invalid.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            bail!("metadata file too short: {} bytes", data.len());
        }

        let magic: [u8; 4] = data[0..4].try_into().unwrap();
        if magic != MAGIC {
            bail!("bad metadata magic: {:?}", magic);
        }

        let version = data[4];
        if version != VERSION {
            bail!("unsupported metadata version: {}", version);
        }

        let chunk_size = u32::from_be_bytes([data[5], data[6], data[7], data[8]]) as usize;
        let num_chunks = u32::from_be_bytes([data[9], data[10], data[11], data[12]]) as usize;
        let file_size = u64::from_be_bytes([
            data[13], data[14], data[15], data[16], data[17], data[18], data[19], data[20],
        ]);
        let full_hash: [u8; 32] = data[21..53].try_into().unwrap();
        let entry_count = u32::from_be_bytes([data[53], data[54], data[55], data[56]]) as usize;

        let expected_len = HEADER_SIZE + entry_count * ENTRY_SIZE;
        if data.len() < expected_len {
            bail!(
                "metadata truncated: expected {} bytes, got {}",
                expected_len,
                data.len()
            );
        }

        let mut chunk_hashes = HashMap::with_capacity(entry_count);
        for i in 0..entry_count {
            let offset = HEADER_SIZE + i * ENTRY_SIZE;
            let idx = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            let hash: [u8; 32] = data[offset + 4..offset + 36].try_into().unwrap();
            chunk_hashes.insert(idx, hash);
        }

        Ok(Self {
            chunk_size,
            num_chunks,
            file_size,
            full_hash,
            chunk_hashes,
        })
    }

    /// Write metadata to disk (atomic: write to temp, then rename).
    pub fn save(&self, parts_dir: &str, filename: &str) -> Result<()> {
        let path = Self::meta_path(parts_dir, filename);
        let tmp = format!("{}.tmp", path);
        let data = self.to_bytes();

        fs::write(&tmp, &data)?;
        if let Err(e) = fs::rename(&tmp, &path) {
            let _ = fs::remove_file(&tmp);
            return Err(e.into());
        }

        Ok(())
    }

    /// Load metadata from disk. Returns `None` if the file doesn't exist.
    pub fn load(parts_dir: &str, filename: &str) -> Result<Option<Self>> {
        let path = Self::meta_path(parts_dir, filename);

        if !Path::new(&path).exists() {
            return Ok(None);
        }

        let data = fs::read(&path)?;
        Ok(Some(Self::from_bytes(&data)?))
    }

    /// Check whether stored transfer parameters match the current request.
    pub fn params_match(
        &self,
        chunk_size: usize,
        num_chunks: usize,
        file_size: u64,
        full_hash: &[u8; 32],
    ) -> bool {
        self.chunk_size == chunk_size
            && self.num_chunks == num_chunks
            && self.file_size == file_size
            && self.full_hash == *full_hash
    }

    /// Verify a `.part` file against its stored hash. Returns `true` if the
    /// hash matches, `false` if the file is missing, corrupt, or has no entry.
    pub fn verify_part(&self, index: usize, parts_dir: &str, filename: &str) -> bool {
        let expected_hash = match self.chunk_hashes.get(&index) {
            Some(h) => h,
            None => return false,
        };

        let part_path = format!("{}/{}.part{}", parts_dir, filename, index);

        let data = match fs::read(&part_path) {
            Ok(d) => d,
            Err(_) => return false,
        };

        let actual_hash: [u8; 32] = blake3::hash(&data).into();

        actual_hash == *expected_hash
    }

    /// Remove metadata file and all associated `.part` files.
    pub fn purge(parts_dir: &str, filename: &str, num_chunks: usize) {
        let meta_path = Self::meta_path(parts_dir, filename);
        let _ = fs::remove_file(&meta_path);

        for i in 0..num_chunks {
            let part_path = format!("{}/{}.part{}", parts_dir, filename, i);
            let _ = fs::remove_file(&part_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty() {
        let meta = TransferMetadata::new(1024, 10, 10000, [0xAA; 32]);
        let bytes = meta.to_bytes();
        let restored = TransferMetadata::from_bytes(&bytes).unwrap();

        assert_eq!(restored.chunk_size, 1024);
        assert_eq!(restored.num_chunks, 10);
        assert_eq!(restored.file_size, 10000);
        assert_eq!(restored.full_hash, [0xAA; 32]);
        assert!(restored.chunk_hashes.is_empty());
    }

    #[test]
    fn roundtrip_with_chunks() {
        let mut meta = TransferMetadata::new(4096, 5, 20000, [0xBB; 32]);
        meta.add_chunk(0, [0x11; 32]);
        meta.add_chunk(2, [0x33; 32]);
        meta.add_chunk(4, [0x55; 32]);

        let bytes = meta.to_bytes();
        let restored = TransferMetadata::from_bytes(&bytes).unwrap();

        assert_eq!(restored.chunk_hashes.len(), 3);
        assert_eq!(restored.chunk_hashes[&0], [0x11; 32]);
        assert_eq!(restored.chunk_hashes[&2], [0x33; 32]);
        assert_eq!(restored.chunk_hashes[&4], [0x55; 32]);
    }

    #[test]
    fn params_match_detects_mismatch() {
        let meta = TransferMetadata::new(1024, 10, 10000, [0xAA; 32]);

        assert!(meta.params_match(1024, 10, 10000, &[0xAA; 32]));
        assert!(!meta.params_match(2048, 10, 10000, &[0xAA; 32])); // different chunk_size
        assert!(!meta.params_match(1024, 5, 10000, &[0xAA; 32])); // different num_chunks
        assert!(!meta.params_match(1024, 10, 9999, &[0xAA; 32])); // different file_size
        assert!(!meta.params_match(1024, 10, 10000, &[0xBB; 32])); // different full_hash
    }

    #[test]
    fn verify_part_detects_corruption() {
        let temp = std::env::temp_dir();
        let dir = temp.to_string_lossy().to_string();
        let fname = "test_verify";

        // Create a .part file
        let part_path = format!("{}/{}.part{}", dir, fname, 0);
        let data = b"hello chunk data for testing";
        let hash: [u8; 32] = blake3::hash(data).into();

        // Write the part file
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&part_path, data).unwrap();

        // Create metadata with correct hash
        let mut meta = TransferMetadata::new(64, 1, data.len() as u64, [0; 32]);
        meta.add_chunk(0, hash);
        assert!(meta.verify_part(0, &dir, fname));

        // Corrupt the part file
        std::fs::write(&part_path, b"corrupted data here!!").unwrap();
        assert!(!meta.verify_part(0, &dir, fname));

        // Cleanup
        let _ = std::fs::remove_file(&part_path);
    }
}
