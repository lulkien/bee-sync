//! Server handler module - coordinates file transfer connections
//!
//! This module provides the main entry points for handling both control and
//! data connections in the file transfer protocol.
//!
//! # Architecture
//!
//! - [`control`]: Handles handshake and transfer coordination
//! - [`data`]: Handles individual chunk data transfers
//!
//! # Usage
//!
//! ```ignore
//! use bee_sync::server::handler;
//! ```

mod control;
mod data;

// Re-export public functions from submodules
pub(super) use control::handle_control_connection;
pub(super) use data::handle_data_connection;
