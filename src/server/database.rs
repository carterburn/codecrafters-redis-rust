use crate::command::{ExecutorResponse, RedisCommand};
use crate::resp::{self, RespValue};
use crate::server::types::{EntryId, RedisDataType, RedisKey, StoredValue};
use anyhow::Result;
use bytes::Bytes;
use std::collections::VecDeque;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot::{self, Receiver};

pub(crate) const INITIAL_CAPACITY: usize = 16;

pub(crate) struct Database {
    /// The data store
    store: HashMap<RedisKey, StoredValue>,

    /// clients waiting on blpop
    blocking: HashMap<RedisKey, VecDeque<oneshot::Sender<Bytes>>>,
}

impl Database {
    pub(crate) fn new() -> Self {
        Self {
            store: HashMap::with_capacity(INITIAL_CAPACITY),
            blocking: HashMap::with_capacity(INITIAL_CAPACITY),
        }
    }

    /// Get a key for a string value
    pub(crate) fn get_key(&self, key: &RedisKey) -> Option<Bytes> {
        self.store.get(key).and_then(|v| {
            if !v.expired(Instant::now()) {
                v.as_string()
            } else {
                None
            }
        })
    }

    fn get_key_expiration(&self, key: &RedisKey) -> Option<Instant> {
        self.store.get(key).and_then(|v| {
            let exp = v.get_expiration()?;
            Some(*exp)
        })
    }

    fn set_key(&mut self, key: &RedisKey, value: StoredValue) -> Option<StoredValue> {
        self.store.insert(key.clone(), value)
    }

    fn remove_key(&mut self, key: &RedisKey) {
        self.store.remove(key);
    }

    fn rpush(&mut self, key: &RedisKey, values: impl Iterator<Item = Bytes>) -> Option<usize> {
        let stored_value = self.store.entry(key.clone()).or_insert_with(|| {
            StoredValue::new(
                RedisDataType::List(VecDeque::with_capacity(INITIAL_CAPACITY)),
                None,
            )
        });

        let list = stored_value.as_list_mut()?;

        list.extend(values);
        let len = list.len();
        self.notify_blocker(key);
        Some(len)
    }

    fn lpush(&mut self, key: &RedisKey, values: impl Iterator<Item = Bytes>) -> Option<usize> {
        let stored_value = self.store.entry(key.clone()).or_insert_with(|| {
            StoredValue::new(
                RedisDataType::List(VecDeque::with_capacity(INITIAL_CAPACITY)),
                None,
            )
        });

        let list = stored_value.as_list_mut()?;

        for v in values {
            list.push_front(v);
        }
        let len = list.len();
        self.notify_blocker(key);
        Some(len)
    }

    fn notify_blocker(&mut self, key: &RedisKey) {
        // notify any blockers that a new key has been pushed somewhere
        tracing::info!("Checking blockers");
        let Some(waiters) = self.blocking.get_mut(key) else {
            tracing::info!("Did not find key {key:?} in blockers");
            return;
        };
        if let Some(ch) = waiters.pop_front() {
            tracing::info!("Found channel; sending signal");
            let Some(v) = self.lpop(key) else {
                tracing::error!("Had a waiter ready, but nothing popped");
                return;
            };
            let _ = ch.send(v);
        }
    }

    fn lrange(&self, key: &RedisKey, start: isize, end: isize) -> Option<Vec<Bytes>> {
        let list = self.store.get(key)?;
        let list = list.as_list()?;
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

    fn llen(&self, key: &RedisKey) -> usize {
        match self.store.get(key) {
            Some(v) => match v.as_list() {
                Some(l) => l.len(),
                None => 0,
            },
            None => 0,
        }
    }

    fn lpop(&mut self, key: &RedisKey) -> Option<Bytes> {
        let stored_value = self.store.get_mut(key)?;

        let list = stored_value.as_list_mut()?;
        list.pop_front()
    }

    fn lpop_many(&mut self, key: &RedisKey, num_pops: usize) -> Option<Vec<Bytes>> {
        let stored_value = self.store.get_mut(key)?;

        let list = stored_value.as_list_mut()?;
        Some(
            (0..num_pops.min(list.len()))
                .filter_map(|_| list.pop_front())
                .collect(),
        )
    }

    fn blpop(&mut self, key: &RedisKey) -> Receiver<Bytes> {
        // otherwise, we need to setup infrastructure to block until we are told something is
        // available on the list
        let (wait_tx, wait_rx) = oneshot::channel();
        // push this sender onto the queue to be notified if a PUSH was made
        // ensure there is no deadlock
        let waiters = self.blocking.entry(key.clone()).or_default();
        waiters.push_back(wait_tx);

        wait_rx
    }

    fn key_type(&self, key: &RedisKey) -> Option<&'static str> {
        Some(match self.store.get(key)?.value {
            RedisDataType::String(_) => "string",
            RedisDataType::List(_) => "list",
            RedisDataType::Stream(_) => "stream",
        })
    }

    fn xadd(&mut self, stream_key: &Bytes, entry_id: &Bytes, pairs: Vec<Bytes>) -> Result<Bytes> {
        let stored_value = self
            .store
            .entry(stream_key.clone())
            .or_insert_with(|| StoredValue::new(RedisDataType::Stream(BTreeMap::new()), None));

        let stream = stored_value
            .as_stream_mut()
            .ok_or(anyhow::anyhow!("Unable to get stream"))?;

        let last_kv = stream.last_key_value();

        let id = EntryId::construct(entry_id, last_kv)?;
        let response = format!("{}", id).into();

        let _ = stream.insert(id, pairs);

        Ok(response)
    }

    pub(crate) fn handle_cmd(&mut self, cmd: RedisCommand) -> Result<ExecutorResponse> {
        match cmd {
            RedisCommand::Ping => Ok(ExecutorResponse::Value(RespValue::SimpleString(
                "PONG".into(),
            ))),
            RedisCommand::Echo(msg) => Ok(ExecutorResponse::Value(RespValue::BulkString(msg))),
            RedisCommand::Get(key) => match self.get_key(&key) {
                Some(v) => {
                    tracing::info!("Returning value: {:?}", v);
                    Ok(ExecutorResponse::Value(RespValue::BulkString(v)))
                }
                _ => Ok(ExecutorResponse::Value(RespValue::NullBulkString)),
            },
            RedisCommand::Set {
                key,
                value,
                expiration,
            } => {
                let exp = expiration.map(|dur| Instant::now() + dur);
                tracing::info!("Set {:?} -> {:?} with expiration at: {exp:?}", key, value);

                let val = StoredValue::new(RedisDataType::String(value), exp);
                self.set_key(&key, val);
                // send our new expiration time to the channel if needed
                // TODO: fix expiration
                // if let Some(time) = exp {
                //     let _ = self.expiration_tx.send((time, key)).await;
                // };
                Ok(ExecutorResponse::Value(RespValue::SimpleString(
                    "OK".into(),
                )))
            }
            RedisCommand::RPush {
                list_name,
                elements,
            } => {
                tracing::info!("RPush to {list_name:?} with elements: {elements:?}");
                let size = self
                    .rpush(&list_name, elements.iter().map(|e| e.clone()))
                    .ok_or(anyhow::anyhow!("Key is not a list"))?;
                Ok(ExecutorResponse::Value(RespValue::Integer(
                    size.try_into()?,
                )))
            }
            RedisCommand::LPush {
                list_name,
                elements,
            } => {
                tracing::info!("LPush to {list_name:?} with elements: {elements:?}");
                let size = self
                    .lpush(&list_name, elements.iter().map(|e| e.clone()))
                    .ok_or(anyhow::anyhow!("Key is not a list"))?;
                Ok(ExecutorResponse::Value(RespValue::Integer(
                    size.try_into()?,
                )))
            }
            RedisCommand::LRange {
                list_name,
                start,
                end,
            } => {
                tracing::info!("LRange on {list_name:?} for range: {start}..={end}");
                if let Some(vals) = self.lrange(&list_name, start, end) {
                    Ok(ExecutorResponse::Value(RespValue::Array(
                        vals.iter()
                            .map(|v| RespValue::BulkString(v.slice(..)))
                            .collect(),
                    )))
                } else {
                    Ok(ExecutorResponse::Value(RespValue::Array(vec![])))
                }
            }
            RedisCommand::LLen { list_name } => {
                tracing::info!("LLen on {list_name:?}");
                Ok(ExecutorResponse::Value(RespValue::Integer(
                    self.llen(&list_name).try_into()?,
                )))
            }
            RedisCommand::LPop {
                list_name,
                num_pops,
            } => {
                tracing::info!("LPop on {list_name:?} with num pops: {num_pops:?}");
                if let Some(num) = num_pops {
                    Ok(ExecutorResponse::Value(
                        self.lpop_many(&list_name, num)
                            .map(|v| {
                                RespValue::Array(
                                    v.iter()
                                        .map(|val| RespValue::BulkString(val.slice(..)))
                                        .collect(),
                                )
                            })
                            .unwrap_or(RespValue::NullBulkString),
                    ))
                } else {
                    Ok(ExecutorResponse::Value(
                        self.lpop(&list_name)
                            .map(|v| RespValue::BulkString(v.slice(..)))
                            .unwrap_or(RespValue::NullBulkString),
                    ))
                }
            }
            RedisCommand::BLPop { list_name, timeout } => {
                tracing::info!("BLPop on {list_name:?} with timeout of: {timeout:?}");
                // first check if there is something in the list to start
                if let Some(v) = self.lpop(&list_name) {
                    return Ok(ExecutorResponse::Value(RespValue::BulkString(v.slice(..))));
                }

                let wait_rx = self.blpop(&list_name);
                Ok(ExecutorResponse::Blocking {
                    rx: wait_rx,
                    key: list_name.clone(),
                    timeout,
                })
            }
            RedisCommand::Type { key_name } => Ok(ExecutorResponse::Value(
                self.key_type(&key_name)
                    .map(|s| RespValue::SimpleString(s.into()))
                    .unwrap_or(RespValue::SimpleString("none".into())),
            )),
            RedisCommand::XAdd {
                stream_key,
                entry_id,
                pairs,
            } => Ok(ExecutorResponse::Value(
                self.xadd(&stream_key, &entry_id, pairs)
                    .map(RespValue::BulkString)?,
            )),
        }
    }
}
