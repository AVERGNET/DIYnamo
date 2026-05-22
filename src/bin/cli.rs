use anyhow::Result;
use clap::{Parser, Subcommand};
use diynamo::client::KvClient;

const DEFAULT_URL: &str = "http://127.0.0.1:8080";

#[derive(Parser)]
#[command(name = "diynamo-cli", about = "CLI client for the DIYnamo KV HTTP API")]
struct Cli {
    /// Base URL of the node (e.g. http://127.0.0.1:8080)
    #[arg(long, short = 'u', default_value = DEFAULT_URL, global = true)]
    url: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// PUT /kv/{key} with JSON body {"value": "..."}
    Put { key: String, value: String },
    /// GET /kv/{key}
    Get { key: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = KvClient::new(&cli.url)?;

    match cli.command {
        Command::Put { key, value } => {
            client.put(&key, &value).await?;
            println!("put ok");
        }
        Command::Get { key } => {
            let response = client.get(&key).await?;
            println!("{}", response.value);
        }
    }

    Ok(())
}
