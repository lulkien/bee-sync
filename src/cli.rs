//! CLI module using clap for command parsing.

use clap::{Parser, Subcommand};

use crate::{client, server};

/// bee-sync - File transfer tool with parallel chunked transfer
#[derive(Parser)]
#[command(name = "bee-sync")]
#[command(about = "bee-sync - File transfer tool with parallel chunked transfer", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run as server
    Server(ServerArgs),
    /// Run as client
    Client(ClientArgs),
}

impl Commands {
    /// Check if verbose mode is enabled
    pub fn verbose(&self) -> bool {
        match self {
            Commands::Server(args) => args.verbose,
            Commands::Client(args) => args.verbose,
        }
    }
}

#[derive(Parser)]
pub struct ServerArgs {
    /// Control port
    #[arg(long, default_value_t = server::CONTROL_PORT)]
    pub port: u16,

    /// Output directory
    #[arg(long, default_value = "./received/")]
    pub output: String,

    /// TLS certificate file (PEM)
    #[arg(long)]
    pub cert: Option<String>,

    /// TLS private key file (PEM)
    #[arg(long)]
    pub key: Option<String>,

    /// Max parallel connections
    #[arg(long, default_value_t = server::MAX_PARALLEL)]
    pub max_parallel: usize,

    /// Enable verbose/debug logging
    #[arg(long)]
    pub verbose: bool,
}

#[derive(Parser)]
pub struct ClientArgs {
    /// Server host
    #[arg(long, default_value = "localhost")]
    pub host: String,

    /// Control port
    #[arg(long, default_value_t = server::CONTROL_PORT)]
    pub port: u16,

    /// File to send (required)
    #[arg(long)]
    pub file: Option<String>,

    /// Chunk size (e.g., 1M, 10M, 1G, 1k; default: 5M)
    #[arg(long, default_value = client::DEFAULT_CHUNK_SIZE)]
    pub chunk_size: String,

    /// Split file into N chunks instead of using --chunk-size
    #[arg(long)]
    pub chunk_count: Option<usize>,

    /// Max parallel connections
    #[arg(long, default_value_t = client::DEFAULT_PARALLEL)]
    pub parallel: usize,

    /// Retries per chunk
    #[arg(long, default_value_t = client::DEFAULT_RETRIES)]
    pub retries: usize,

    /// Enable TLS encryption
    #[arg(long)]
    pub tls: bool,

    /// Disable TLS certificate verification
    #[arg(long)]
    pub tls_no_verify: bool,

    /// Enable verbose/debug logging
    #[arg(long)]
    pub verbose: bool,
}
