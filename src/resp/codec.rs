use bytes::{Buf, BufMut, BytesMut};
use nom::AsBytes;
use tokio_util::codec::{Decoder, Encoder};

use crate::resp::{parse::parse_resp, RespValue};

pub struct RespFrame;

impl Decoder for RespFrame {
    type Item = RespValue;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match parse_resp(src) {
            Ok((remaining, value)) => {
                let consumed = src.len() - remaining.len();
                src.advance(consumed);
                Ok(Some(value))
            }
            Err(nom::Err::Incomplete(_)) => Ok(None),
            Err(e) => Err(anyhow::anyhow!("Parsing error: {e:?}")),
        }
    }
}

impl RespFrame {
    /// Pull out the encoder function as an associated function to be able to recursively call it
    /// for arrays
    fn encode_value(item: RespValue, dst: &mut BytesMut) -> Result<(), anyhow::Error> {
        const NULL_ARRAY_STRING_LEN: usize = 5;
        const SIMPLE_VALUE_START_LEN: usize = 3;
        const BULK_STRING_START_LEN: usize = 5;
        const ARRAY_START_LEN: usize = 3;
        const CRLF: [u8; 2] = *b"\r\n";

        match item {
            RespValue::NullArray => {
                dst.reserve(NULL_ARRAY_STRING_LEN);
                dst.extend_from_slice(&b"*-1\r\n"[..]);
            }
            RespValue::NullBulkString => {
                dst.reserve(NULL_ARRAY_STRING_LEN);
                dst.extend_from_slice(&b"$-1\r\n"[..]);
            }
            RespValue::SimpleString(s) => {
                dst.reserve(SIMPLE_VALUE_START_LEN + s.len());
                dst.put_u8(b'+');
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(&CRLF[..]);
            }
            RespValue::SimpleError(e) => {
                dst.reserve(SIMPLE_VALUE_START_LEN + e.len());
                dst.put_u8(b'-');
                dst.extend_from_slice(e.as_bytes());
                dst.extend_from_slice(&CRLF[..]);
            }
            RespValue::Integer(i) => {
                let i_str = i.to_string();
                dst.reserve(SIMPLE_VALUE_START_LEN + i_str.len());
                dst.put_u8(b':');
                dst.extend_from_slice(i_str.as_bytes());
                dst.extend_from_slice(&CRLF[..]);
            }
            RespValue::BulkString(s) => {
                let len_str = s.len().to_string();
                dst.reserve(BULK_STRING_START_LEN + len_str.len() + s.len());
                dst.put_u8(b'$');
                dst.extend_from_slice(len_str.as_bytes());
                dst.extend_from_slice(&CRLF[..]);
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(&CRLF[..]);
            }
            RespValue::Array(v) => {
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

impl Encoder<RespValue> for RespFrame {
    type Error = anyhow::Error;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        RespFrame::encode_value(item, dst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_simple_string() {
        let mut buf = BytesMut::with_capacity(1024);
        let item = RespValue::SimpleString("OK".to_string());
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"+OK\r\n"[..]);
    }

    #[test]
    fn encode_simple_error() {
        let mut buf = BytesMut::with_capacity(1024);
        let item = RespValue::SimpleError("Error message".to_string());
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"-Error message\r\n"[..]);
    }

    #[test]
    fn encode_integer() {
        let mut buf = BytesMut::with_capacity(1024);

        // encoding 0
        let item = RespValue::Integer(0);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b":0\r\n"[..]);
        buf.clear();

        // encoding 1000
        let item = RespValue::Integer(1000);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b":1000\r\n"[..]);
        buf.clear();

        // encoding negative values
        let item = RespValue::Integer(-1);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b":-1\r\n"[..]);
    }

    #[test]
    fn encode_bulk_string() {
        let mut buf = BytesMut::with_capacity(1024);

        // null bulk string
        let item = RespValue::NullBulkString;
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"$-1\r\n"[..]);
        buf.clear();

        // empty string
        let item = RespValue::BulkString("".to_string());
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"$0\r\n\r\n"[..]);
        buf.clear();

        // basic bulk string
        let item = RespValue::BulkString("hello".to_string());
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"$5\r\nhello\r\n"[..]);
    }

    #[test]
    fn encode_array() {
        let mut buf = BytesMut::with_capacity(1024);

        // null array
        let item = RespValue::NullArray;
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"*-1\r\n"[..]);
        buf.clear();

        // walk through official RESP spec array tests
        // empty array
        let item = RespValue::Array(vec![]);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"*0\r\n"[..]);
        buf.clear();

        // array of two bulk strings "hello" and "world"
        let item = RespValue::Array(vec![
            RespValue::BulkString("hello".to_string()),
            RespValue::BulkString("world".to_string()),
        ]);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"[..]);
        buf.clear();

        // array of three integers
        let item = RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Integer(2),
            RespValue::Integer(3),
        ]);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(&buf[..], &b"*3\r\n:1\r\n:2\r\n:3\r\n"[..]);
        buf.clear();

        // mixed array types
        let item = RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Integer(2),
            RespValue::Integer(3),
            RespValue::Integer(4),
            RespValue::BulkString("hello".to_string()),
        ]);
        RespFrame::encode_value(item, &mut buf).unwrap();
        assert_eq!(
            &buf[..],
            &b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n"[..]
        );
        buf.clear();

        // nested array
        let item = RespValue::Array(vec![
            RespValue::Array(vec![
                RespValue::Integer(1),
                RespValue::Integer(2),
                RespValue::Integer(3),
            ]),
            RespValue::Array(vec![
                RespValue::SimpleString("Hello".to_string()),
                RespValue::SimpleError("World".to_string()),
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
