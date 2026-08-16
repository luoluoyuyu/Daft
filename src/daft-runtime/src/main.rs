use std::{net::SocketAddr, str::FromStr};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let address =
        std::env::var("DAFT_RUNTIME_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let address = SocketAddr::from_str(&address).map_err(std::io::Error::other)?;
    println!("daft-runtime listening on http://{address}");
    let token = std::env::var("DAFT_RUNTIME_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    if token.is_some() {
        println!("daft-runtime: authentication enabled (DAFT_RUNTIME_TOKEN)");
    }
    daft_runtime::serve(address, token).await
}
