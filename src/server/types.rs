use std::{
    collections::{HashMap, VecDeque},
    time::Instant,
};

use bytes::Bytes;

pub(crate) type RedisKey = Bytes;

#[derive(Clone, Debug)]
pub enum RedisDataType {
    String(Bytes),
    List(VecDeque<Bytes>),
    Stream(HashMap<Bytes, Vec<Bytes>>),
}

#[derive(Clone, Debug)]
pub(crate) struct StoredValue {
    /// The actual value
    pub value: RedisDataType,

    /// Last set time (if key was set with expirations)
    expiration: Option<Instant>,
}

impl StoredValue {
    pub(crate) fn new(value: RedisDataType, expiration: Option<Instant>) -> Self {
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

    pub(crate) fn get_value(&self) -> &RedisDataType {
        &self.value
    }

    pub(crate) fn get_expiration(&self) -> Option<&Instant> {
        self.expiration.as_ref()
    }

    pub(crate) fn as_string(&self) -> Option<Bytes> {
        match &self.value {
            RedisDataType::String(s) => Some(s.slice(..)),
            _ => None,
        }
    }

    pub(crate) fn as_list(&self) -> Option<&VecDeque<Bytes>> {
        match &self.value {
            RedisDataType::List(l) => Some(l),
            _ => None,
        }
    }

    pub(crate) fn as_list_mut(&mut self) -> Option<&mut VecDeque<Bytes>> {
        match &mut self.value {
            RedisDataType::List(l) => Some(l),
            _ => None,
        }
    }

    pub(crate) fn as_stream(&self) -> Option<&HashMap<Bytes, Vec<Bytes>>> {
        match &self.value {
            RedisDataType::Stream(s) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn as_stream_mut(&mut self) -> Option<&mut HashMap<Bytes, Vec<Bytes>>> {
        match &mut self.value {
            RedisDataType::Stream(s) => Some(s),
            _ => None,
        }
    }
}

pub(crate) type ExpiryEvent = (Instant, RedisKey);
