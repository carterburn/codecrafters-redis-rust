use core::str;

use bytes::Bytes;

pub mod codec;
mod parse;

#[derive(Debug, PartialEq, Clone)]
pub enum RedisValue {
    SimpleString(Bytes),
    SimpleError(Bytes),
    Integer(i64),
    NullBulkString,
    BulkString(Bytes),
    NullArray,
    Array(Vec<RedisValue>),
}

impl TryFrom<RedisValue> for String {
    type Error = anyhow::Error;

    fn try_from(value: RedisValue) -> Result<Self, Self::Error> {
        match value {
            RedisValue::BulkString(s) => Ok(str::from_utf8(&s[..])?.to_uppercase()),
            _ => Err(anyhow::anyhow!("Invalid conversion to string")),
        }
    }
}

impl TryFrom<&RedisValue> for String {
    type Error = anyhow::Error;

    fn try_from(value: &RedisValue) -> Result<Self, Self::Error> {
        match value {
            RedisValue::BulkString(s) => Ok(str::from_utf8(&s[..])?.to_uppercase()),
            _ => Err(anyhow::anyhow!("Invalid conversion to string")),
        }
    }
}

impl TryFrom<&RedisValue> for Bytes {
    type Error = anyhow::Error;

    fn try_from(value: &RedisValue) -> Result<Self, Self::Error> {
        match value {
            RedisValue::BulkString(s) => Ok(s.slice(..)),
            _ => Err(anyhow::anyhow!("Invalid RedisType, expected BulkString")),
        }
    }
}
