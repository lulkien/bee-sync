//! bee-sync - Rust implementation of a file transfer system
//!
//! Architecture:
//! - Control channel: single TCP/TLS connection for handshake (port 19999)
//! - Data channels: Data ports 45000-46000 allocated per chunk (parallel transfer)
//! - Resume: server tracks received chunks, client queries before sending

mod cli;
mod client;
mod file_ops;
mod logger;
mod protocol;
mod server;
mod utils;

use clap::Parser;
use std::process;

use crate::{
    cli::{Cli, Commands},
    client::ClientConfig,
    logger::init_logging,
    server::ServerConfig,
};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    init_logging(cli.command.verbose());

    let result = match cli.command {
        Commands::Server(args) => run_server(args).await,
        Commands::Client(args) => run_client(args).await,
    };

    process::exit(result);
}

async fn run_server(args: cli::ServerArgs) -> i32 {
    log::info!("Starting bee-sync server on port {}", args.port);
    log::info!("Output directory: {}", args.output);

    let config = ServerConfig {
        port: args.port,
        output_dir: args.output,
        certfile: args.cert,
        keyfile: args.key,
        max_parallel: args.max_parallel,
    };

    match server::run_server(config).await {
        Ok(_) => {
            log::info!("Server stopped");
            0
        }
        Err(e) => {
            log::error!("Server error: {}", e);
            1
        }
    }
}

async fn run_client(args: cli::ClientArgs) -> i32 {
    let filepath = match args.file {
        Some(f) => f,
        None => {
            eprintln!("Error: --file is required");
            return 1;
        }
    };

    let file_size = {
        let meta = match std::fs::metadata(&filepath) {
            Ok(m) => m,
            Err(e) => {
                log::error!("Failed to read file metadata: {}", e);
                return 1;
            }
        };
        meta.len()
    };

    let chunk_size = if let Some(count) = args.chunk_count {
        if count == 0 {
            log::error!("Chunk count must be > 0");
            return 1;
        }
        let cs = (file_size as usize).div_ceil(count);
        log::info!("Starting bee-sync client");
        log::info!("Server: {}:{}", args.host, args.port);
        log::info!(
            "File: {} ({} bytes, {} chunks of ~{} bytes each)",
            filepath, file_size, count, cs
        );
        cs
    } else {
        let cs = match utils::parse_chunk_size(&args.chunk_size) {
            Ok(s) => s,
            Err(e) => {
                log::error!("Invalid chunk size: {}", e);
                return 1;
            }
        };
        log::info!("Starting bee-sync client");
        log::info!("Server: {}:{}", args.host, args.port);
        log::info!("File: {} (chunk size: {})", filepath, cs);
        cs
    };

    let config = ClientConfig {
        host: args.host,
        port: args.port,
        filepath,
        chunk_size,
        parallel: args.parallel,
        retries: args.retries,
        tls: args.tls,
        tls_no_verify: args.tls_no_verify,
        verbose: args.verbose,
    };

    let result = client::run_client(config).await;

    if result == 0 {
        log::info!("Transfer completed successfully");
    } else {
        log::error!("Transfer failed");
    }

    result
}
