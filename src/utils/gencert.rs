use anyhow::Context;
use anyhow::Result;
use std::path::Path;
use wtransport::Identity;

const CERT_FILE: &str = "localhost.pem";
const KEY_FILE: &str = "localhost-key.pem";

pub async fn load_or_generate_identity() -> Result<Identity> {
    let cert_path = Path::new(CERT_FILE);
    let key_path = Path::new(KEY_FILE);

    if cert_path.exists() && key_path.exists() {
        println!("📜 Loading existing certificate from disk...");

        let identity_result = Identity::load_pemfiles(cert_path, key_path).await;

        match identity_result {
            Ok(identity) => {
                println!("✅ Successfully loaded existing certificate");
                return Ok(identity);
            }
            Err(e) => {
                println!("⚠️ Failed to load existing certificate: {}", e);
                println!("🔄 Generating new certificate...");
            }
        }
    }

    println!("🔐 Generating new self-signed certificate...");
    let identity = Identity::self_signed(["localhost", "127.0.0.1", "::1"])
        .context("cannot create self signed identity")?;

    println!("💾 Storing certificate to file: '{CERT_FILE}'");
    identity
        .certificate_chain()
        .store_pemfile(CERT_FILE)
        .await
        .context("cannot store certificate chain")?;

    println!("💾 Storing private key to file: '{KEY_FILE}'");
    identity
        .private_key()
        .store_secret_pemfile(KEY_FILE)
        .await
        .context("cannot store private key")?;

    Ok(identity)
}
