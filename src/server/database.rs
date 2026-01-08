use crate::server::types::{RedisKey, Value};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

pub(crate) const INITIAL_CAPACITY: usize = 16;

pub(crate) struct Database {
    /// Basic Key/Value store
    kv: Arc<DashMap<RedisKey, Value>>,

    /// List support
    lists: Arc<DashMap<RedisKey, VecDeque<Value>>>,

    /// clients waiting on blpop
    blocking: Arc<DashMap<RedisKey, VecDeque<oneshot::Sender<bool>>>>,
}

impl Database {
    pub(crate) fn new() -> Self {
        Self {
            kv: Arc::new(DashMap::with_capacity(INITIAL_CAPACITY)),
            lists: Arc::new(DashMap::with_capacity(INITIAL_CAPACITY)),
            blocking: Arc::new(DashMap::with_capacity(INITIAL_CAPACITY)),
        }
    }

    pub(crate) fn get_key(&self, key: &RedisKey) -> Option<Bytes> {
        self.kv.get(key).and_then(|v| {
            if !v.expired(Instant::now()) {
                Some(v.get_value())
            } else {
                None
            }
        })
    }

    pub(crate) fn get_key_expiration(&self, key: &RedisKey) -> Option<Instant> {
        self.kv.get(key).and_then(|v| {
            let exp = v.get_expiration()?;
            Some(*exp)
        })
    }

    pub(crate) fn set_key(&self, key: &RedisKey, value: Value) -> Option<Value> {
        self.kv.insert(key.clone(), value)
    }

    pub(crate) fn remove_key(&self, key: &RedisKey) {
        self.kv.remove(key);
    }

    pub(crate) fn rpush(&self, key: &RedisKey, value: impl Iterator<Item = Value>) -> usize {
        let mut list = self
            .lists
            .entry(key.clone())
            .or_insert(VecDeque::with_capacity(INITIAL_CAPACITY));
        list.extend(value);
        self.notify_blocker(key);
        list.len()
    }

    pub(crate) fn lpush(&self, key: &RedisKey, value: impl Iterator<Item = Value>) -> usize {
        let mut list = self
            .lists
            .entry(key.clone())
            .or_insert(VecDeque::with_capacity(INITIAL_CAPACITY));
        for v in value {
            list.push_front(v);
        }
        self.notify_blocker(key);
        list.len()
    }

    fn notify_blocker(&self, key: &RedisKey) {
        // notify any blockers that a new key has been pushed somewhere
        tracing::info!("Checking blockers");
        let Some(mut waiters) = self.blocking.get_mut(key) else {
            tracing::info!("Did not find key {key:?} in blockers");
            return;
        };
        if let Some(ch) = waiters.pop_front() {
            tracing::info!("Found channel; sending signal");
            let _ = ch.send(true);
        }
    }

    pub(crate) fn lrange(&self, key: &RedisKey, start: isize, end: isize) -> Option<Vec<Value>> {
        let list = self.lists.get(key)?;
        // adjust negative start / end to actual indices
        let start: usize = if start < 0 {
            let abs_start = start.abs().try_into().ok()?;
            list.len().saturating_sub(abs_start)
        } else {
            start.try_into().ok()?
        };
        let mut end: usize = if end < 0 {
            let abs_end = end.abs().try_into().ok()?;
            list.len().saturating_sub(abs_end)
        } else {
            end.try_into().ok()?
        };

        if start >= list.len() {
            return None;
        }
        if end >= list.len() {
            // inclusive ranges so truncate to the last element's index
            end = list.len() - 1;
        }
        if start > end {
            return None;
        }

        let (head, tail) = list.as_slices();
        Some(if start >= head.len() {
            // all indices are in tail, so just adjust the indices by head.len()
            tracing::info!("Indices in tail: {start} {end} {}", head.len());
            tail[start - head.len()..=end - head.len()].to_vec()
        } else if end >= head.len() {
            tracing::info!("Split indices: {start} {end} {}", head.len());
            // start starts in head and consumes the rest, then finishes off in tail
            let mut first = head[start..].to_vec();
            let second = tail[..=end - head.len()].to_vec();
            first.extend(second);
            first
        } else {
            tracing::info!("Indices in head: {start} {end} {}", head.len());
            // both start and end exist in head, so can just grip and rip
            head[start..=end].to_vec()
        })
    }

    pub(crate) fn llen(&self, key: &RedisKey) -> usize {
        match self.lists.get(key) {
            Some(l) => l.len(),
            None => 0,
        }
    }

    pub(crate) fn lpop(&self, key: &RedisKey) -> Option<Value> {
        let mut list = self.lists.get_mut(key)?;
        list.pop_front()
    }

    pub(crate) fn lpop_many(&self, key: &RedisKey, num_pops: usize) -> Option<Vec<Value>> {
        let mut list = self.lists.get_mut(key)?;
        Some(
            (0..num_pops.min(list.len()))
                .filter_map(|_| list.pop_front())
                .collect(),
        )
    }

    pub(crate) async fn blpop(&self, key: &RedisKey, timeout: f64) -> Option<Value> {
        {
            // ensure we don't have deadlock by doing this in a block
            let mut list = self.lists.get_mut(key)?;

            // if the list is NOT empty, just do normal LPOP operation
            if !list.is_empty() {
                return list.pop_front();
            }
        }

        // otherwise, we need to setup infrastructure to block until we are told something is
        // available on the list
        let (wait_tx, wait_rx) = oneshot::channel();
        // push this sender onto the queue to be notified if a PUSH was made
        {
            // ensure there is no deadlock
            let mut waiters = self.blocking.entry(key.clone()).or_default();
            waiters.push_back(wait_tx);
        }

        // wait for the timeout or something to be added to the list
        tokio::select! {
            _ = async {
                if timeout == 0.0 {
                    tracing::info!("Indefinite wait on {key:?}");
                    std::future::pending::<()>().await
                } else {
                    tracing::info!("Waiting on {key:?} for {timeout}");
                    tokio::time::sleep_until(tokio::time::Instant::from_std(Instant::now() + Duration::from_secs_f64(timeout))).await
                }
            } => {
                // timeout reached!
                None
            },
            Ok(_) = wait_rx => {
                // value add to the list! check and pop
                let mut list = self.lists.get_mut(key)?;
                if !list.is_empty() {
                    list.pop_front()
                } else {
                    None
                }
            }
        }
    }
}
