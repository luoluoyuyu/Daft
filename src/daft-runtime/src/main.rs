use std::{net::SocketAddr, str::FromStr};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Executor mode: register with a scheduler and execute polled tasks.
    // Enabled with --executor or DAFT_EXECUTOR=1; the scheduler itself keeps
    // running the HTTP control plane (default mode).
    let executor_mode = std::env::var("DAFT_EXECUTOR")
        .map(|value| value != "0" && value.to_lowercase() != "false")
        .unwrap_or(false)
        || std::env::args().any(|arg| arg == "--executor");
    if executor_mode {
        let scheduler_address = std::env::var("DAFT_SCHEDULER_ADDRESS")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());
        let flight_ip = std::env::var("DAFT_FLIGHT_IP")
            .unwrap_or_else(|_| "127.0.0.1".to_string());
        let token = std::env::var("DAFT_RUNTIME_TOKEN")
            .ok()
            .filter(|t| !t.is_empty());
        println!("daft-runtime running as executor (scheduler {scheduler_address})");
        return daft_runtime::executor::run(&scheduler_address, &flight_ip, token)
            .await
            .map_err(std::io::Error::other);
    }

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
