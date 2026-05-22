use anyhow::Result;
use clap::Parser;
use diynamo::cluster::{run_live_set_printer, GossipNode};
use std::{net::SocketAddr, sync::Arc, time::Duration};

#[derive(Parser)]
#[command(
    name = "diynamo-gossip",
    about = "Run a memberlist gossip node and print the live member set"
)]
struct Args {
    /// Unique node name in the cluster
    #[arg(long)]
    node_id: String,

    /// Gossip bind address (memberlist TCP/UDP), e.g. 127.0.0.1:7946
    #[arg(long)]
    gossip_bind: SocketAddr,

    /// Seed node(s) to join (repeat flag or comma-separated). Omit on the first seed.
    #[arg(long = "join", value_delimiter = ',')]
    join: Vec<SocketAddr>,

    /// Seconds between live-set prints
    #[arg(long, default_value = "1")]
    print_interval_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!(
        "node_id={} gossip_bind={} join={:?}",
        args.node_id, args.gossip_bind, args.join
    );

    let node = GossipNode::start(&args.node_id, args.gossip_bind, &args.join).await?;

    let printer_view: Arc<GossipNode> = node.clone();
    let interval = Duration::from_secs(args.print_interval_secs);
    tokio::spawn(async move {
        run_live_set_printer(printer_view, interval).await;
    });

    // Give the cluster a moment to converge before first tick matters much.
    tokio::time::sleep(Duration::from_millis(500)).await;

    tokio::signal::ctrl_c().await?;
    println!("\nshutting down...");
    node.shutdown().await?;
    Ok(())
}
