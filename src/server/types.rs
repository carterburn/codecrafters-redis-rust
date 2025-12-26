use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use dashmap::DashMap;

pub(crate) type RedisKey = Bytes;

pub(crate) struct Value {
    /// The actual value
    value: Bytes,

    /// Last set time (if key was set with expirations)
    expiration: Option<Instant>,
}

impl Value {
    pub(crate) fn new(value: Bytes, expiration: Option<Instant>) -> Self {
        Self { value, expiration }
    }

    pub(crate) fn expired(&self, current: Instant) -> bool {
        if let Some(expiration) = self.expiration {
            if current >= expiration {
                // key is now expired
                true
            } else {
                // key still valid
                false
            }
        } else {
            // Not expired because it can't
            false
        }
    }

    pub(crate) fn get_value(&self) -> Bytes {
        self.value.slice(..)
    }

    pub(crate) fn get_expiration(&self) -> Option<&Instant> {
        self.expiration.as_ref()
    }
}

pub(crate) type ExpiryEvent = (Instant, RedisKey);

pub(crate) const INITIAL_CAPACITY: usize = 16;

pub(crate) struct Database {
    /// Basic Key/Value store
    kv: Arc<DashMap<RedisKey, Value>>,

    /// List support
    lists: Arc<DashMap<RedisKey, Vec<Value>>>,
}

impl Database {
    pub(crate) fn new() -> Self {
        Self {
            kv: Arc::new(DashMap::with_capacity(INITIAL_CAPACITY)),
            lists: Arc::new(DashMap::with_capacity(INITIAL_CAPACITY)),
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
            Some(exp.clone())
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
            .or_insert(Vec::with_capacity(INITIAL_CAPACITY));
        list.extend(value);
        list.len()
    }

    pub(crate) fn kv(&self) -> Arc<DashMap<RedisKey, Value>> {
        self.kv.clone()
    }

    pub(crate) fn lists(&self) -> Arc<DashMap<RedisKey, Vec<Value>>> {
        self.lists.clone()
    }
}
