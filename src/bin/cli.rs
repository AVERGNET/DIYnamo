use anyhow::{bail, Context, Result};
use clap::Parser;
use diynamo::client::KvClient;
use std::io::{self, Write};

const DEFAULT_URL: &str = "http://127.0.0.1:8080";

#[derive(Parser)]
#[command(
    name = "diynamo-cli",
    about = "Interactive CLI client for the DIYnamo KV HTTP API"
)]
struct Args {
    /// Base URL of the node (e.g. http://127.0.0.1:8080)
    #[arg(long, short = 'u', default_value = DEFAULT_URL)]
    url: String,
}

fn print_help() {
    println!(
        r#"Commands:
  put <key> <value>   Store a string value (value may contain spaces)
  get <key>           Fetch a value
  help                Show this message
  exit | quit         Exit the CLI"#
    );
}

async fn run_command(client: &KvClient, line: &str) -> Result<()> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(());
    }

    let mut parts = line.split_whitespace();
    let cmd = parts.next().context("expected a command")?;

    match cmd {
        "help" | "?" => print_help(),
        "exit" | "quit" => std::process::exit(0),
        "put" => {
            let key = parts.next().context("usage: put <key> <value>")?;
            let value: String = parts.collect::<Vec<_>>().join(" ");
            if value.is_empty() {
                bail!("usage: put <key> <value>");
            }
            client.put(key, &value).await?;
            println!("put ok");
        }
        "get" => {
            let key = parts.next().context("usage: get <key>")?;
            if parts.next().is_some() {
                bail!("usage: get <key>");
            }
            let response = client.get(key).await?;
            println!("{}", response.value);
        }
        other => bail!("unknown command: {other} (try help)"),
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = KvClient::new(&args.url)?;

    println!("DIYnamo CLI connected to {}", args.url);
    print_help();
    println!();

    let stdin = io::stdin();
    loop {
        print!("diynamo> ");
        io::stdout().flush()?;

        let mut line = String::new();
        let bytes = stdin.read_line(&mut line)?;
        if bytes == 0 {
            // EOF (Ctrl-D)
            println!();
            break;
        }

        if let Err(err) = run_command(&client, &line).await {
            eprintln!("error: {err:#}");
        }
    }

    Ok(())
}
