//! Utility functions for Rusync.

use anyhow::Result;

/// Parse chunk size string to bytes
/// Supports suffixes: k, m, g (case-insensitive)
pub fn parse_chunk_size(value: &str) -> Result<usize> {
    let value = value.trim().to_lowercase();
    if value.is_empty() {
        return Err(anyhow::anyhow!("Empty chunk size string"));
    }
    let last_char = value.chars().last().unwrap();
    if ['k', 'm', 'g'].contains(&last_char) {
        let num_str = &value[..value.len() - 1];
        let num: f64 = num_str
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid chunk size: {}", value))?;
        let multiplier = match last_char {
            'k' => 1024.0,
            'm' => 1024.0 * 1024.0,
            'g' => 1024.0 * 1024.0 * 1024.0,
            _ => 1.0,
        };
        Ok((num * multiplier) as usize)
    } else {
        value
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid chunk size: {}", value))
    }
}
