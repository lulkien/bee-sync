//! TLS module for bee-sync client.
//!
//! Provides TLS context loading and configuration.

use std::sync::Arc;

use anyhow::anyhow;
use rustls::{
    ClientConfig, DigitallySignedStruct, Error, RootCertStore,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;

/// Trait for stream that can be used for protocol communication
pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Stream for T {}

/// Certificate verifier that accepts any certificate (for --tls-no-verify)
#[derive(Debug)]
pub struct NoCertVerifier;

impl ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dsa_signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dsa_signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

/// Connect to server with optional TLS
pub async fn connect_to_server(
    host: String,
    port: u16,
    use_tls: bool,
    tls_no_verify: bool,
) -> anyhow::Result<Box<dyn Stream>> {
    if !use_tls {
        let stream = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|e| anyhow!("Connection failed: {}", e))?;

        return Ok(Box::new(stream));
    }

    let mut config = ClientConfig::builder()
        .with_root_certificates(RootCertStore::from_iter(
            webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
        ))
        .with_no_client_auth();

    if tls_no_verify {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoCertVerifier));
    }

    let connector = TlsConnector::from(Arc::new(config));
    let dns_name = ServerName::try_from(host.clone())?;
    let stream = TcpStream::connect((host.as_str(), port)).await?;
    let tls_stream = connector.connect(dns_name, stream).await?;

    Ok(Box::new(tls_stream))
}
