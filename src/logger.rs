//! Logger module for fern-based logging initialization.

use fern::Dispatch;
use log::LevelFilter;

/// Initialize logging with specified verbosity level
pub fn init_logging(verbose: bool) {
    let level = if verbose {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    };

    Dispatch::new()
        .format(|out, message, record| out.finish(format_args!("[{}] {}", record.level(), message)))
        .level(level)
        .chain(std::io::stderr())
        .apply()
        .ok();
}
