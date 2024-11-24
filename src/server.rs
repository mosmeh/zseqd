mod commands;

use crate::{
    redis::{RedisError, RespCodec, RespError},
    Args,
};
use anyhow::{anyhow, bail};
use bstr::ByteSlice;
use bytes::Bytes;
use dashmap::DashMap;
use etcd_client::{Compare, CompareOp, KvClient, PutOptions, Txn, TxnOp, TxnOpResponse};
use futures::{SinkExt, StreamExt};
use std::{
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_util::codec::Framed;
use tracing::{debug, info};

#[derive(Default)]
struct Sequence {
    value: AtomicU64,
    max: AtomicU64,
    state: Arc<Mutex<SequenceState>>,
}

struct SequenceState {
    /// Whether the sequence is contained in the sequences map.
    linked: bool,
}

impl Default for SequenceState {
    fn default() -> Self {
        Self { linked: true }
    }
}

pub struct Server {
    alloc_chunk_size: u64,
    red_zone_size: u64,
    etcd: etcd_client::Client,
    node_id: u64,
    lease_id: i64,
    sequences: DashMap<Bytes, Arc<Sequence>>,
    node_cache: DashMap<u64, SocketAddr>,
}

pub async fn serve(server: &Arc<Server>, conn: TcpStream) -> std::io::Result<()> {
    let mut framed = Framed::new(conn, RespCodec::new());
    while let Some(decoded) = framed.next().await {
        let strings = match decoded {
            Ok(strings) => strings,
            Err(RespError::Io(err)) => return Err(err),
            Err(RespError::Protocol(err)) => {
                return framed
                    .send(Err(format!("Protocol error: {}", err).into()))
                    .await;
            }
        };
        let Some((command, args)) = strings.split_first() else {
            // Empty request
            continue;
        };
        let result = match command.to_ascii_uppercase().as_slice() {
            b"PING" => Ok("PONG".into()),
            b"QUIT" => return Ok(()),
            b"INCR" => commands::incr(server, args).await,
            b"INCRBY" => commands::incrby(server, args).await,
            b"LEASE.REVOKE" => commands::lease_revoke(server, args).await,
            _ => {
                let command = String::from_utf8_lossy(command).into_owned();
                Err(format!("unknown command '{0:.128}'", command).into())
            }
        };
        framed.send(result).await?;
    }
    Ok(())
}

impl Server {
    pub async fn new(args: &Args) -> anyhow::Result<Self> {
        if args.red_zone > args.alloc_chunk.get() {
            bail!("red zone size must be <= alloc chunk size");
        }
        if args.lease_ttl.get() <= args.lease_keep_alive_interval.get() {
            bail!("lease TTL must be > lease keep-alive interval");
        }

        let etcd = etcd_client::Client::connect(&args.etcd, None).await?;

        let mut kv_client = etcd.kv_client();
        let node_id = generate_node_id(&mut kv_client).await?;
        info!("Node ID: {}", node_id);

        let lease_ttl = args.lease_ttl.get().try_into()?;
        let mut lease_client = etcd.lease_client();
        let lease_id = lease_client.grant(lease_ttl, None).await?.id();
        {
            let interval = Duration::from_secs(args.lease_keep_alive_interval.get());
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let (mut keeper, mut stream) = lease_client.keep_alive(lease_id).await?;
            tokio::spawn(async move {
                loop {
                    keeper.keep_alive().await.unwrap();
                    stream.message().await.unwrap();
                    interval.tick().await;
                }
            });
        }

        let advertised_addr = args.advertise.unwrap_or(args.bind);
        kv_client
            .put(
                node_key(node_id),
                advertised_addr.to_string(),
                PutOptions::new().with_lease(lease_id).into(),
            )
            .await?;

        Ok(Self {
            alloc_chunk_size: args.alloc_chunk.get(),
            red_zone_size: args.red_zone,
            etcd,
            node_id,
            lease_id,
            sequences: DashMap::new(),
            node_cache: DashMap::new(),
        })
    }

    async fn alloc_range(&self, seq_id: &[u8], seq: &Sequence) -> Result<(), RedisError> {
        debug!("Allocating range for {:?}", seq_id.as_bstr());

        let seq_key = seq_key(seq_id);
        let lease_key = lease_key(seq_id);

        let lease_txn = Txn::new()
            .when([Compare::mod_revision(
                lease_key.clone(),
                CompareOp::Equal,
                0,
            )])
            .and_then([
                TxnOp::put(
                    lease_key.clone(),
                    self.node_id.to_string(),
                    PutOptions::new().with_lease(self.lease_id).into(),
                ),
                TxnOp::get(seq_key.clone(), None),
            ])
            .or_else([
                TxnOp::get(lease_key.clone(), None),
                TxnOp::get(seq_key.clone(), None),
            ]);

        let mut kv_client = self.etcd.kv_client();
        loop {
            // Acquire the lease or confirm that we already have it.
            let txn_response = kv_client.txn(lease_txn.clone()).await?;
            let txn_responses = txn_response.op_responses();
            if txn_response.succeeded() {
                debug!("Acquired lease");
            } else {
                let op_response = txn_responses.first().ok_or("missing response")?;
                let node_id: u64 = parse_get_response(op_response)?;
                if node_id == self.node_id {
                    debug!("Already have the lease");
                } else {
                    match self.resolve_node(node_id).await? {
                        Some(addr) => {
                            debug!("Another node {} has the lease. Redirecting", node_id);
                            return Err(RedisError::Moved { slot: 0, addr });
                        }
                        None => {
                            debug!(
                                "Another node {} has the lease, but the node disappeared. Retrying",
                                node_id
                            );
                            continue;
                        }
                    }
                }
            }

            // Allocate the range
            let op_response = txn_responses.get(1).ok_or("missing response")?;
            let TxnOpResponse::Get(get_response) = op_response else {
                return Err("unexpected response type".into());
            };
            let old_max = if let Some(kv) = get_response.kvs().first() {
                kv.value_str()?.parse()?
            } else {
                0
            };
            let new_max = old_max + self.alloc_chunk_size;
            let txn = Txn::new()
                .when([Compare::value(
                    lease_key.clone(),
                    CompareOp::Equal,
                    self.node_id.to_string(),
                )])
                .and_then([TxnOp::put(seq_key.clone(), new_max.to_string(), None)]);
            let txn_response = kv_client.txn(txn).await?;
            if !txn_response.succeeded() {
                debug!("Failed to allocate range. Retrying");
                continue;
            }
            debug!("Allocated range: {}..{}", old_max, new_max);
            if seq.value.load(Ordering::SeqCst) == 0 {
                // Load the initial value
                seq.value.store(old_max, Ordering::SeqCst);
            }
            seq.max.fetch_max(new_max, Ordering::SeqCst);
            return Ok(());
        }
    }

    /// Resolves the address of a node by its ID.
    ///
    /// Returns `None` if the node is not found.
    async fn resolve_node(&self, node_id: u64) -> anyhow::Result<Option<SocketAddr>> {
        if let Some(addr) = self.node_cache.get(&node_id) {
            return Ok(Some(*addr));
        }
        debug!("Resolving node address: {}", node_id);
        let mut kv_client = self.etcd.kv_client();
        let response = kv_client.get(node_key(node_id), None).await?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let addr = kv.value_str()?.parse()?;
        self.node_cache.insert(node_id, addr);
        Ok(Some(addr))
    }
}

/// Key that stores the address of a node.
fn node_key(node_id: u64) -> Vec<u8> {
    format!("/node/{}", node_id).into_bytes()
}

/// Key that stores the maximum allocated value of a sequence.
fn seq_key(seq_id: &[u8]) -> Vec<u8> {
    let mut key = b"/seq/".to_vec();
    key.extend_from_slice(seq_id);
    key
}

/// Key that stores the node ID of the lease holder.
fn lease_key(seq_id: &[u8]) -> Vec<u8> {
    let mut key = seq_key(seq_id);
    key.extend_from_slice(b"/lease");
    key
}

/// Generates a unique node ID.
async fn generate_node_id(kv_client: &mut KvClient) -> anyhow::Result<u64> {
    const NEXT_ID_KEY: &[u8] = b"/node/next_id";

    // Create the next_id key if it doesn't exist.
    let txn = Txn::new()
        .when([Compare::mod_revision(NEXT_ID_KEY, CompareOp::Equal, 0)])
        .and_then([TxnOp::put(NEXT_ID_KEY, "0", None)])
        .or_else([TxnOp::get(NEXT_ID_KEY, None)]);
    let txn_response = kv_client.txn(txn).await?;
    let mut next_id: u64 = if txn_response.succeeded() {
        0
    } else {
        let op_responses = txn_response.op_responses();
        let op_response = op_responses
            .first()
            .ok_or_else(|| anyhow!("missing get response"))?;
        parse_get_response(op_response)?
    };

    loop {
        let txn = Txn::new()
            .when([Compare::value(
                NEXT_ID_KEY,
                CompareOp::Equal,
                next_id.to_string(),
            )])
            .and_then([TxnOp::put(NEXT_ID_KEY, (next_id + 1).to_string(), None)])
            .or_else([TxnOp::get(NEXT_ID_KEY, None)]);
        let txn_response = kv_client.txn(txn).await?;
        if txn_response.succeeded() {
            return Ok(next_id);
        }

        // Another node has updated next_id. Retry.
        let op_responses = txn_response.op_responses();
        let op_response = op_responses
            .first()
            .ok_or_else(|| anyhow!("missing get response"))?;
        next_id = parse_get_response(op_response)?;
    }
}

fn parse_get_response<T: FromStr>(op_response: &TxnOpResponse) -> anyhow::Result<T> {
    let TxnOpResponse::Get(get_response) = op_response else {
        bail!("unexpected response type");
    };
    let kv = get_response
        .kvs()
        .first()
        .ok_or_else(|| anyhow!("missing get response"))?;
    kv.value_str()?
        .parse()
        .map_err(|_| anyhow!("failed to parse get response"))
}
