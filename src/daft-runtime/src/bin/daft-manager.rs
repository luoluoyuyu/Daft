use std::{net::SocketAddr, str::FromStr};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let address = std::env::var("DAFT_MANAGER_ADDRESS")
        .or_else(|_| std::env::var("DAFT_RUNTIME_ADDRESS"))
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let address = SocketAddr::from_str(&address).map_err(std::io::Error::other)?;
    let token = std::env::var("DAFT_RUNTIME_TOKEN")
        .ok()
        .filter(|value| !value.is_empty());
    println!("daft-manager listening on http://{address}");
    daft_runtime::serve(address, token).await
}
