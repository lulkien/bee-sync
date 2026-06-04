//! bee-sync - Rust implementation of a file transfer system
//!
//! Architecture:
//! - Control channel: single TCP/TLS connection for handshake (port 19999)
//! - Data channels: Data ports 45000-46000 allocated per chunk (parallel transfer)
//! - Resume: server tracks received chunks, client queries before sending

mod cli;
mod client;
mod file_ops;
mod protocol;
mod server;
mod utils;

use clap::Parser;
use fern::Dispatch;
use log::{LevelFilter, error, info};
use std::{fs, io, process};

use crate::{
    cli::{Cli, Commands},
    client::ClientConfig,
    server::ServerConfig,
};

#[tokio::main]
async fn main() {
    // Install default CryptoProvider before any TLS operations.
    // Required when both ring and aws-lc-rs features are compiled in.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();

    init_logging(cli.command.verbose());

    let result = match cli.command {
        Commands::Server(args) => run_server(args).await,
        Commands::Client(args) => run_client(args).await,
    };

    process::exit(result);
}

/// Initialize logging with specified verbosity level
fn init_logging(verbose: bool) {
    let level = if verbose {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    };

    Dispatch::new()
        .format(|out, message, record| out.finish(format_args!("[{}] {}", record.level(), message)))
        .level(level)
        .chain(io::stderr())
        .apply()
        .ok();
}

async fn run_server(args: cli::ServerArgs) -> i32 {
    let (bind_host, port) = match utils::parse_address(&args.address) {
        Ok((h, p)) => (h, p),
        Err(e) => {
            error!("Invalid --address: {}", e);
            return 1;
        }
    };

    let temp_dir = args.temp_dir.unwrap_or_else(|| args.output_dir.clone());

    info!("Starting bee-sync server on {}:{}", bind_host, port);
    info!("Output directory: {}", args.output_dir);
    info!("Temp directory: {}", temp_dir);

    let config = ServerConfig {
        bind_host,
        port,
        output_dir: args.output_dir,
        temp_dir,
        certfile: args.cert,
        keyfile: args.key,
        max_parallel: args.max_parallel,
    };

    match server::run_server(config).await {
        Ok(_) => {
            info!("Server stopped");
            0
        }
        Err(e) => {
            error!("Server error: {}", e);
            1
        }
    }
}

async fn run_client(args: cli::ClientArgs) -> i32 {
    let filepath = match (args.file, args.url) {
        (Some(f), None) => f,
        (None, Some(url)) => match utils::download_file(&url, &args.temp_dir, args.verbose).await {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to download from URL: {}", e);
                return 1;
            }
        },
        (None, None) => {
            eprintln!("Error: --file or --url is required");
            return 1;
        }
        (Some(_), Some(_)) => {
            // clap enforces mutual exclusion, but be defensive
            eprintln!("Error: --file and --url are mutually exclusive");
            return 1;
        }
    };

    let file_size = {
        let meta = match fs::metadata(&filepath) {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to read file metadata: {}", e);
                return 1;
            }
        };
        meta.len()
    };

    let (host, port) = match utils::parse_address(&args.address) {
        Ok((h, p)) => (h, p),
        Err(e) => {
            error!("Invalid --address: {}", e);
            return 1;
        }
    };

    let chunk_size = if let Some(count) = args.chunk_count {
        if count == 0 {
            error!("Chunk count must be > 0");
            return 1;
        }
        (file_size as usize).div_ceil(count)
    } else {
        let size_str = args.chunk_size.as_deref().unwrap_or(crate::client::DEFAULT_CHUNK_SIZE);
        match utils::parse_chunk_size(size_str) {
            Ok(s) => s,
            Err(e) => {
                error!("Invalid chunk size: {}", e);
                return 1;
            }
        }
    };

    let num_chunks = (file_size as usize).div_ceil(chunk_size);
    info!("Starting bee-sync client");
    info!("Server: {}:{}", host, port);
    info!(
        "File: {} ({} bytes, {} chunks of {} bytes each)",
        filepath, file_size, num_chunks, chunk_size,
    );

    let config = ClientConfig {
        host,
        port,
        filepath,
        chunk_size,
        parallel: args.parallel,
        retries: args.retries,
        tls: args.tls,
        tls_no_verify: args.tls_no_verify,
        verbose: args.verbose,
    };

    // run_client handles its own outcome + elapsed logging
    client::run_client(config).await
}
