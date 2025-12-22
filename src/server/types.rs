use std::time::Instant;

use bytes::Bytes;

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
}

pub(crate) type ExpiryEvent = (Instant, RedisKey);
