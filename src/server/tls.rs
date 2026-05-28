//! TLS module for Rusync server.
//!
//! Provides TLS context loading and configuration.

use std::{fs::File, io::BufReader, sync::Arc};

use anyhow::Result;
use rustls::{ServerConfig, pki_types::CertificateDer};

/// Load TLS context from cert and key files
pub fn load_tls_context(certfile: &str, keyfile: &str) -> Result<Arc<ServerConfig>> {
    let cert_file = File::open(certfile)?;
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(cert_file)).collect::<Result<Vec<_>, _>>()?;

    let key_file = File::open(keyfile)?;
    let mut key_reader = BufReader::new(key_file);
    let keys = rustls_pemfile::private_key(&mut key_reader)?;
    let key = match keys {
        Some(k) => k,
        None => return Err(anyhow::anyhow!("No private key found")),
    };

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(Arc::new(config))
}
