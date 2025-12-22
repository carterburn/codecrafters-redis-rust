use bytes::{BufMut, BytesMut};
use nom::AsBytes;
use tokio_util::codec::{Decoder, Encoder};

use crate::resp::{parse::parse, RedisValue};

pub struct RespFrame;

impl Decoder for RespFrame {
    type Item = RedisValue;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match parse(src, 0).map_err(|e| anyhow::anyhow!("Parsing error: {e:?}"))? {
            Some((pos, intermediate)) => {
                let parsed = src.split_to(pos);
                Ok(Some(intermediate.generate_value(&parsed.freeze())))
            }
            None => Ok(None),
        }
    }
}

impl RespFrame {
    /// Pull out the encoder function as an associated function to be able to recursively call it
    /// for arrays
    fn encode_value(item: RedisValue, dst: &mut BytesMut) -> Result<(), anyhow::Error> {
        const NULL_ARRAY_STRING_LEN: usize = 5;
        const SIMPLE_VALUE_START_LEN: usize = 3;
        const BULK_STRING_START_LEN: usize = 5;
        const ARRAY_START_LEN: usize = 3;
        const CRLF: [u8; 2] = *b"\r\n";

        match item {
            RedisValue::NullArray => {
                dst.reserve(NULL_ARRAY_STRING_LEN);
                dst.extend_from_slice(&b"*-1\r\n"[..]);
            }
            RedisValue::NullBulkString => {
                dst.reserve(NULL_ARRAY_STRING_LEN);
                dst.extend_from_slice(&b"$-1\r\n"[..]);
            }
            RedisValue::SimpleString(s) => {
                dst.reserve(SIMPLE_VALUE_START_LEN + s.len());
                dst.put_u8(b'+');
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(&CRLF[..]);
            }
            RedisValue::SimpleError(e) => {
                dst.reserve(SIMPLE_VALUE_START_LEN + e.len());
                dst.put_u8(b'-');
                dst.extend_from_slice(e.as_bytes());
                dst.extend_from_slice(&CRLF[..]);
            }
            RedisValue::Integer(i) => {
                let i_str = i.to_string();
                dst.reserve(SIMPLE_VALUE_START_LEN + i_str.len());
                dst.put_u8(b':');
                dst.extend_from_slice(i_str.as_bytes());
                dst.extend_from_slice(&CRLF[..]);
            }
            RedisValue::BulkString(s) => {
                let len_str = s.len().to_string();
                dst.reserve(BULK_STRING_START_LEN + len_str.len() + s.len());
                dst.put_u8(b'$');
                dst.extend_from_slice(len_str.as_bytes());
                dst.extend_from_slice(&CRLF[..]);
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(&CRLF[..]);
            }
            RedisValue::Array(v) => {
                let len_str = v.len().to_string();
                dst.reserve(ARRAY_START_LEN + len_str.len());
                dst.put_u8(b'*');
                dst.extend_from_slice(len_str.as_bytes());
                dst.extend_from_slice(&CRLF[..]);
                for element in v {
                    RespFrame::encode_value(element, dst)?;
                }
            }
        }
        Ok(())
    }
}

impl Encoder<RedisValue> for RespFrame {
    type Error = anyhow::Error;

    fn encode(&mut self, item: RedisValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        RespFrame::encode_value(item, dst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_simple_string() {
        let mut buf = BytesMut::with_capacity(1024);
        let item = RedisValue::SimpleString("OK".into());
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"+OK\r\n"[..]);
    }

    #[test]
    fn encode_simple_error() {
        let mut buf = BytesMut::with_capacity(1024);
        let item = RedisValue::SimpleError("Error message".into());
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"-Error message\r\n"[..]);
    }

    #[test]
    fn encode_integer() {
        let mut buf = BytesMut::with_capacity(1024);

        // encoding 0
        let item = RedisValue::Integer(0);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b":0\r\n"[..]);
        buf.clear();

        // encoding 1000
        let item = RedisValue::Integer(1000);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b":1000\r\n"[..]);
        buf.clear();

        // encoding negative values
        let item = RedisValue::Integer(-1);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b":-1\r\n"[..]);
    }

    #[test]
    fn encode_bulk_string() {
        let mut buf = BytesMut::with_capacity(1024);

        // null bulk string
        let item = RedisValue::NullBulkString;
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"$-1\r\n"[..]);
        buf.clear();

        // empty string
        let item = RedisValue::BulkString("".into());
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"$0\r\n\r\n"[..]);
        buf.clear();

        // basic bulk string
        let item = RedisValue::BulkString("hello".into());
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"$5\r\nhello\r\n"[..]);
    }

    #[test]
    fn encode_array() {
        let mut buf = BytesMut::with_capacity(1024);

        // null array
        let item = RedisValue::NullArray;
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"*-1\r\n"[..]);
        buf.clear();

        // walk through official RESP spec array tests
        // empty array
        let item = RedisValue::Array(vec![]);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"*0\r\n"[..]);
        buf.clear();

        // array of two bulk strings "hello" and "world"
        let item = RedisValue::Array(vec![
            RedisValue::BulkString("hello".into()),
            RedisValue::BulkString("world".into()),
        ]);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"[..]);
        buf.clear();

        // array of three integers
        let item = RedisValue::Array(vec![
            RedisValue::Integer(1),
            RedisValue::Integer(2),
            RedisValue::Integer(3),
        ]);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"*3\r\n:1\r\n:2\r\n:3\r\n"[..]);
        buf.clear();

        // mixed array types
        let item = RedisValue::Array(vec![
            RedisValue::Integer(1),
            RedisValue::Integer(2),
            RedisValue::Integer(3),
            RedisValue::Integer(4),
            RedisValue::BulkString("hello".into()),
        ]);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(
            &buf[..],
            &b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n"[..]
        );
        buf.clear();

        // nested array
        let item = RedisValue::Array(vec![
            RedisValue::Array(vec![
                RedisValue::Integer(1),
                RedisValue::Integer(2),
                RedisValue::Integer(3),
            ]),
            RedisValue::Array(vec![
                RedisValue::SimpleString("Hello".into()),
                RedisValue::SimpleError("World".into()),
            ]),
        ]);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(
            &buf[..],
            &b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n"[..]
        );
        buf.clear();
    }
}
