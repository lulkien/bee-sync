//! CLI module using clap for command parsing.

use clap::{Parser, Subcommand};

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
    /// Bind address (host:port)
    #[arg(short = 'a', long, default_value = "0.0.0.0:19999")]
    pub address: String,

    /// Output directory for final assembled files
    #[arg(short = 'o', long = "output-dir", default_value = "./received/")]
    pub output_dir: String,

    /// Temporary directory for in-progress .part files (default: same as --output-dir)
    #[arg(short = 't', long = "temp-dir")]
    pub temp_dir: Option<String>,

    /// TLS certificate file (PEM)
    #[arg(short = 'c', long)]
    pub cert: Option<String>,

    /// TLS private key file (PEM)
    #[arg(short = 'k', long)]
    pub key: Option<String>,

    /// Max parallel connections per transfer
    #[arg(short = 'm', long = "max-parallel", default_value_t = crate::server::MAX_PARALLEL)]
    pub max_parallel: usize,

    /// Enable verbose/debug logging
    #[arg(short = 'v', long)]
    pub verbose: bool,
}

#[derive(Parser)]
pub struct ClientArgs {
    /// Server address (host:port)
    #[arg(short = 'a', long, default_value = "localhost:19999")]
    pub address: String,

    /// File to send
    #[arg(short = 'f', long)]
    pub file: Option<String>,

    /// Chunk size (e.g., 1M, 10M, 1G; default: 5M)
    #[arg(short = 's', long = "chunk-size", default_value = crate::client::DEFAULT_CHUNK_SIZE)]
    pub chunk_size: String,

    /// Split file into N chunks instead of using --chunk-size
    #[arg(short = 'n', long = "chunk-count")]
    pub chunk_count: Option<usize>,

    /// Max parallel data connections
    #[arg(short = 'p', long, default_value_t = crate::client::DEFAULT_PARALLEL)]
    pub parallel: usize,

    /// Retries per chunk
    #[arg(short = 'r', long, default_value_t = crate::client::DEFAULT_RETRIES)]
    pub retries: usize,

    /// Enable TLS encryption
    #[arg(long)]
    pub tls: bool,

    /// Disable TLS certificate verification
    #[arg(long = "tls-no-verify")]
    pub tls_no_verify: bool,

    /// Enable verbose/debug logging
    #[arg(short = 'v', long)]
    pub verbose: bool,
}
