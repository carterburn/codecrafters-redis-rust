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
