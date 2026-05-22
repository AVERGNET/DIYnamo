use anyhow::Result;
use axum::{
    extract::Path,
    routing::{get, put},
    Json, Router,
};
use clap::Parser;
use diynamo::api::types::{GetResponse, PutBody};

const STUB_VALUE: &str = "stub-value";

#[derive(Parser)]
#[command(name = "diynamo-server", about = "DIYnamo KV HTTP server (stub)")]
struct Args {
    #[arg(long, default_value = "8080")]
    port: u16,
}

async fn put_kv(Path(key): Path<String>, Json(body): Json<PutBody>) -> &'static str {
    println!("put request received: key={key}, value={}", body.value);
    "ok"
}

async fn get_kv(Path(key): Path<String>) -> Json<GetResponse> {
    println!("get request received: key={key}");
    Json(GetResponse {
        value: STUB_VALUE.to_string(),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let addr = format!("0.0.0.0:{}", args.port);

    let app = Router::new()
        .route("/kv/{key}", put(put_kv).get(get_kv));

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("listening on http://{addr}");
    axum::serve(listener, app).await?;

    Ok(())
}
