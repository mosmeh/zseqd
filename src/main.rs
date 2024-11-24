mod redis;
mod server;

use clap::Parser;
use server::Server;
use std::{net::SocketAddr, num::NonZeroU64, sync::Arc};
use tokio::net::TcpListener;
use tracing::{debug, error, info};

#[derive(Parser)]
struct Args {
    /// Listening address
    #[clap(long, default_value = "127.0.0.1:12345")]
    bind: SocketAddr,

    /// Address to advertise to clients and other nodes
    #[clap(long)]
    advertise: Option<SocketAddr>,

    /// etcd endpoints
    #[clap(long, default_value = "127.0.0.1:2379")]
    etcd: Vec<String>,

    /// Number of sequence values to allocate at a time.
    ///
    /// Larger values allows for more efficient allocation but may increase
    /// the gap between allocated values in case of a node failure.
    #[clap(long, default_value = "10000")]
    alloc_chunk: NonZeroU64,

    /// Start allocating a new range when less than this many values remain
    /// in the current allocated range
    #[clap(long, default_value = "5000")]
    red_zone: u64,

    /// Lease time-to-live period in seconds.
    ///
    /// Larger values reduce the risk of a lease expiring while a node is
    /// still alive, but may increase the time until the lease can be
    /// re-acquired by another node in case of a node failure.
    #[clap(long, default_value = "10")]
    lease_ttl: NonZeroU64,

    /// Lease keep-alive interval in seconds
    #[clap(long, default_value = "1")]
    lease_keep_alive_interval: NonZeroU64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let listener = TcpListener::bind(args.bind).await?;
    info!("Listening on {}", args.bind);

    let server = Arc::new(Server::new(&args).await?);
    loop {
        let (socket, _) = listener.accept().await?;
        let server = server.clone();
        tokio::spawn(async move {
            match server::serve(&server, socket).await {
                Ok(()) => debug!("Connection closed"),
                Err(err) => error!("Connection error: {}", err),
            }
        });
    }
}
