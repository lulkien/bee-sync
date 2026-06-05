//! Utility functions for bee-sync.

use std::io::IsTerminal;
use std::path::Path;

use anyhow::{Result, anyhow};
use tokio::io::AsyncWriteExt;

/// Parse an address string in "host:port" format.
/// Returns (host, port) on success.
pub fn parse_address(addr: &str) -> Result<(String, u16)> {
    let (host, port_str) = addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("Invalid address format: expected host:port, got '{}'", addr))?;

    let port: u16 = port_str
        .parse()
        .map_err(|_| anyhow!("Invalid port in address: '{}'", addr))?;

    Ok((host.to_string(), port))
}

/// Parse chunk size string to bytes
/// Supports suffixes: k, m, g (case-insensitive)
pub fn parse_chunk_size(value: &str) -> Result<usize> {
    let value = value.trim().to_lowercase();

    if value.is_empty() {
        return Err(anyhow!("Empty chunk size string"));
    }

    let last_char = value.chars().last().unwrap();

    if ['k', 'm', 'g'].contains(&last_char) {
        let num_str = &value[..value.len() - 1];

        let num: f64 = num_str
            .parse()
            .map_err(|_| anyhow!("Invalid chunk size: {}", value))?;

        let multiplier = match last_char {
            'k' => 1024.0,
            'm' => 1024.0 * 1024.0,
            'g' => 1024.0 * 1024.0 * 1024.0,
            _ => 1.0,
        };

        let bytes = num * multiplier;
        // Guard against overflow when casting to usize
        if bytes > usize::MAX as f64 {
            return Err(anyhow!(
                "Chunk size too large: {} (max {} bytes)",
                value,
                usize::MAX
            ));
        }
        Ok(bytes as usize)
    } else {
        value
            .parse()
            .map_err(|_| anyhow!("Invalid chunk size: {}", value))
    }
}

/// Download a file from a URL to a temp directory with progress bar.
///
/// Creates the temp directory if it doesn't exist. Extracts filename from URL
/// path component. Returns the full path to the downloaded file.
pub async fn download_file(url: &str, temp_dir: &str, verbose: bool) -> Result<String> {
    let dir = Path::new(temp_dir);
    tokio::fs::create_dir_all(dir).await?;

    // Extract filename from URL path. Walk segments right-to-left,
    // picking the last non-empty segment that looks like a filename.
    // Fall back to "download" if URL ends with '/' or has no path.
    let path_part = url.split('?').next().unwrap_or("");
    let segments: Vec<&str> = path_part.split('/').filter(|s| !s.is_empty()).collect();
    let filename = segments
        .last()
        .filter(|s| s.contains('.'))
        .copied()
        .unwrap_or("download");
    let dest = dir.join(filename);

    let host = url.split('/').nth(2).unwrap_or("?");
    log::info!("Downloading from {host} -> {}", dest.display());

    let response = reqwest::get(url).await?;
    let total = response.content_length().unwrap_or(0);

    let mut file = tokio::fs::File::create(&dest).await?;
    let mut stream = response.bytes_stream();

    use futures_util::StreamExt;

    // Progress reporting: indicatif bar on TTY, periodic logs otherwise
    let use_tty = !verbose && std::io::stderr().is_terminal();
    let pb = if use_tty {
        let bar = indicatif::ProgressBar::new(total);
        bar.set_style(
            indicatif::ProgressStyle::default_bar()
                .template(
                    "[{bar:40}] {percent:>3}% {bytes:>10}/{total_bytes} {bytes_per_sec} eta {eta}",
                )
                .unwrap(),
        );
        Some(bar)
    } else {
        None
    };

    let mut downloaded: u64 = 0;
    let mut last_log = std::time::Instant::now();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;

        if let Some(ref bar) = pb {
            bar.inc(chunk.len() as u64);
        } else if last_log.elapsed() >= std::time::Duration::from_secs(3) {
            let pct = if total > 0 {
                downloaded as f64 / total as f64 * 100.0
            } else {
                0.0
            };
            log::info!("Download progress: {:.1}% ({}/{})", pct, downloaded, total);
            last_log = std::time::Instant::now();
        }
    }

    if let Some(bar) = pb {
        bar.finish();
    }
    log::info!("Downloaded {} bytes ({})", total, filename);
    Ok(dest.to_string_lossy().to_string())
}
