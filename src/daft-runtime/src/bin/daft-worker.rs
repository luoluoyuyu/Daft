#[tokio::main]
async fn main() -> std::io::Result<()> {
    let manager_address = std::env::var("DAFT_MANAGER_URL")
        .or_else(|_| std::env::var("DAFT_SCHEDULER_ADDRESS"))
        .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());
    let flight_ip = std::env::var("DAFT_FLIGHT_IP").unwrap_or_else(|_| "127.0.0.1".to_string());
    let token = std::env::var("DAFT_RUNTIME_TOKEN")
        .ok()
        .filter(|value| !value.is_empty());
    println!("daft-worker connecting to {manager_address}");
    daft_runtime::executor::run(&manager_address, &flight_ip, token)
        .await
        .map_err(std::io::Error::other)
}
