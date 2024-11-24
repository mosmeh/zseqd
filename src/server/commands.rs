use super::{lease_key, Server};
use crate::{
    redis::{RedisError, RedisValue},
    server::Sequence,
};
use bstr::ByteSlice;
use bytes::Bytes;
use dashmap::Entry;
use etcd_client::{Compare, CompareOp, Txn, TxnOp};
use std::sync::{atomic::Ordering, Arc};
use tracing::{debug, warn};

/// `INCR`: Increments a sequence value by 1.
///
/// Returns the value after the increment.
pub async fn incr(server: &Arc<Server>, args: &[Bytes]) -> Result<RedisValue, RedisError> {
    let [seq_id] = args else {
        return Err("wrong number of arguments".into());
    };
    incr_impl(server, seq_id, 1).await
}

/// `INCRBY`: Increments a sequence value by a specified amount.
///
/// Returns the value after the increment.
/// Unlike `INCRBY` in Redis, this command does not support negative increments.
pub async fn incrby(server: &Arc<Server>, args: &[Bytes]) -> Result<RedisValue, RedisError> {
    let [seq_id, amount] = args else {
        return Err("wrong number of arguments".into());
    };
    let amount: u64 = amount
        .to_str()
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or("value is not an integer or out of range")?;
    incr_impl(server, seq_id, amount).await
}

async fn incr_impl(
    server: &Arc<Server>,
    seq_id: &Bytes,
    amount: u64,
) -> Result<RedisValue, RedisError> {
    'retry_unlinked: loop {
        let entry = server.sequences.entry(seq_id.clone());
        let seq = match entry {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                debug!("New or unknown sequence {:?}", seq_id.as_bstr());
                let seq = Arc::new(Sequence::default());
                entry.insert(seq.clone());
                seq
            }
        };
        let value = 'retry_updated: loop {
            let mut value = seq.value.load(Ordering::SeqCst);
            let mut max = seq.max.load(Ordering::SeqCst);
            if value + amount > max {
                if max > 0 {
                    warn!("Allocated range exhausted. Blocking until new range is allocated");
                }
                let state = seq.state.lock().await;
                if !state.linked {
                    debug!("Sequence object got unlinked while waiting for allocation. Retrying");
                    continue 'retry_unlinked;
                }
                value = seq.value.load(Ordering::SeqCst);
                max = seq.max.load(Ordering::SeqCst);
                if value + amount >= max {
                    server.alloc_range(seq_id, &seq).await?;
                    continue 'retry_updated;
                }
            }
            let new_value = value + amount;
            if seq
                .value
                .compare_exchange(value, new_value, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                continue 'retry_updated;
            }
            if new_value + server.red_zone_size > max {
                if let Ok(state) = seq.state.clone().try_lock_owned() {
                    if state.linked {
                        debug!("Red zone reached. Allocating in background");
                        let server = server.clone();
                        let seq_id = seq_id.to_vec();
                        let seq = seq.clone();
                        tokio::spawn(async move {
                            let _state = state;
                            server.alloc_range(&seq_id, &seq).await.unwrap();
                        });
                    } else {
                        debug!("Red zone reached, but sequence object got unlinked. Not going to allocate");
                    }
                }
            }
            break new_value;
        };
        let value: i64 = value.try_into().map_err(|_| "value is out of range")?;
        return Ok(value.into());
    }
}

/// `LEASE.REVOKE`: Revokes a lease this node holds on a sequence.
///
/// Returns 1 if the lease was revoked, 0 if the lease was not found.
pub async fn lease_revoke(server: &Arc<Server>, args: &[Bytes]) -> Result<RedisValue, RedisError> {
    let [seq_id] = args else {
        return Err("wrong number of arguments".into());
    };

    let seq = match server.sequences.entry(seq_id.clone()) {
        Entry::Occupied(entry) => entry.get().clone(),
        Entry::Vacant(_) => return Ok(0.into()),
    };

    let mut kv_client = server.etcd.kv_client();
    let txn = Txn::new()
        .when([Compare::value(
            lease_key(seq_id),
            CompareOp::Equal,
            server.node_id.to_string(),
        )])
        .and_then([TxnOp::delete(lease_key(seq_id), None)]);

    let mut state = seq.state.lock().await;
    state.linked = false;
    seq.max.store(0, Ordering::SeqCst);
    seq.value.store(0, Ordering::SeqCst);
    kv_client.txn(txn).await?;
    server.sequences.remove(seq_id).unwrap();

    Ok(1.into())
}
