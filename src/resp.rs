pub mod codec;
mod parse;

#[derive(Clone, Debug, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    NullBulkString,
    Array(Vec<RespValue>),
    NullArray,
}
