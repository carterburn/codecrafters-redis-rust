use bytes::{Bytes, BytesMut};

use crate::resp::RedisValue;

use std::{num::ParseIntError, str::Utf8Error};

#[derive(Debug)]
pub enum RespParseError {
    IOError(std::io::Error),
    ParseUtf8Error(Utf8Error),
    ParseIntegerError(ParseIntError),
    InvalidFirstByte,
    InvalidBulkStringLength(i64),
    ExceededMaxLength,
    InvalidArrayLength(i64),
}

impl From<std::io::Error> for RespParseError {
    fn from(value: std::io::Error) -> Self {
        Self::IOError(value)
    }
}

impl From<ParseIntError> for RespParseError {
    fn from(value: ParseIntError) -> Self {
        Self::ParseIntegerError(value)
    }
}

impl From<Utf8Error> for RespParseError {
    fn from(value: Utf8Error) -> Self {
        Self::ParseUtf8Error(value)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct BufRange(usize, usize);

#[derive(Debug, PartialEq)]
pub(crate) enum RedisIntermediate {
    SimpleString(BufRange),
    SimpleError(BufRange),
    Integer(i64),
    NullBulkString,
    BulkString(BufRange),
    NullArray,
    Array(Vec<RedisIntermediate>),
}

impl RedisIntermediate {
    pub(crate) fn generate_value(self, buffer: &Bytes) -> RedisValue {
        match self {
            Self::SimpleString(br) => RedisValue::SimpleString(buffer.slice(br.0..br.1)),
            Self::SimpleError(br) => RedisValue::SimpleError(buffer.slice(br.0..br.1)),
            Self::Integer(i) => RedisValue::Integer(i),
            Self::NullBulkString => RedisValue::NullBulkString,
            Self::BulkString(br) => RedisValue::BulkString(buffer.slice(br.0..br.1)),
            Self::NullArray => RedisValue::NullArray,
            Self::Array(intermediates) => RedisValue::Array(
                intermediates
                    .into_iter()
                    .map(|int| int.generate_value(buffer))
                    .collect(),
            ),
        }
    }
}

type ParseResult = Result<Option<(usize, RedisIntermediate)>, RespParseError>;

fn parse_word(input: &BytesMut, pos: usize) -> Option<(usize, BufRange)> {
    if input.len() <= pos {
        return None;
    }
    memchr::memchr(b'\r', &input[pos..]).and_then(|ret| {
        if ret + 1 < input.len() && input[pos + ret + 1] == b'\n' {
            Some((pos + ret + 2, BufRange(pos, pos + ret)))
        } else {
            None
        }
    })
}

fn parse_simple_string(input: &BytesMut, pos: usize) -> ParseResult {
    Ok(parse_word(input, pos).map(|(p, split)| (p, RedisIntermediate::SimpleString(split))))
}

fn parse_simple_error(input: &BytesMut, pos: usize) -> ParseResult {
    Ok(parse_word(input, pos).map(|(p, split)| (p, RedisIntermediate::SimpleError(split))))
}

fn int(input: &BytesMut, pos: usize) -> Result<Option<(usize, i64)>, RespParseError> {
    match parse_word(input, pos) {
        Some((p, int_range)) => {
            let s = str::from_utf8(&input[int_range.0..int_range.1])?;
            Ok(Some((p, s.parse()?)))
        }
        None => Ok(None),
    }
}

fn parse_integer(input: &BytesMut, pos: usize) -> ParseResult {
    Ok(int(input, pos)?.map(|(p, int)| (p, RedisIntermediate::Integer(int))))
}

fn parse_bulk_string(input: &BytesMut, pos: usize) -> ParseResult {
    match int(input, pos)? {
        Some((p, -1)) => Ok(Some((p, RedisIntermediate::NullBulkString))),
        Some((p, length)) if length >= 0 => {
            if length > u32::MAX as i64 {
                return Err(RespParseError::ExceededMaxLength);
            }
            let end = p + length as usize;
            if input.len() < end + 2 {
                Ok(None)
            } else {
                Ok(Some((
                    end + 2,
                    RedisIntermediate::BulkString(BufRange(p, end)),
                )))
            }
        }
        Some((_p, invalid_length)) => Err(RespParseError::InvalidBulkStringLength(invalid_length)),
        None => Ok(None),
    }
}

fn parse_array(input: &BytesMut, pos: usize) -> ParseResult {
    match int(input, pos)? {
        Some((p, -1)) => Ok(Some((p, RedisIntermediate::NullArray))),
        Some((mut p, length)) if length >= 0 => {
            if length > u32::MAX as i64 {
                return Err(RespParseError::ExceededMaxLength);
            }
            let mut values = Vec::with_capacity(length as usize);
            for _ in 0..length {
                match parse(input, p)? {
                    Some((new_p, v)) => {
                        p = new_p;
                        values.push(v);
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((p, RedisIntermediate::Array(values))))
        }
        Some((_p, invalid_length)) => Err(RespParseError::InvalidArrayLength(invalid_length)),
        None => Ok(None),
    }
}

pub(crate) fn parse(input: &BytesMut, pos: usize) -> ParseResult {
    if input.is_empty() {
        return Ok(None);
    }

    if input.len() <= pos {
        return Ok(None);
    }

    match input[pos] {
        b'+' => parse_simple_string(input, pos + 1),
        b'-' => parse_simple_error(input, pos + 1),
        b':' => parse_integer(input, pos + 1),
        b'$' => parse_bulk_string(input, pos + 1),
        b'*' => parse_array(input, pos + 1),
        _ => Err(RespParseError::InvalidFirstByte),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_parse(input: &[u8]) -> RedisValue {
        let mut buf = BytesMut::from(input);
        let (pos, intermediate) = parse(&buf, 0).unwrap().unwrap();
        let parsed = buf.split_to(pos);
        intermediate.generate_value(&parsed.freeze())
    }

    fn setup_result(input: &[u8]) -> ParseResult {
        let buf = BytesMut::from(input);
        parse(&buf, 0)
    }

    #[test]
    fn test_parse() {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let (pos, v) = parse(&buf, 0).unwrap().unwrap();
        assert_eq!(pos, 11);
        assert_eq!(v, RedisIntermediate::BulkString(BufRange(4, 9)));
        // how we would use it in the decoder is below
        let parsed = buf.split_to(pos);
        let value = v.generate_value(&parsed.freeze());
        assert_eq!(value, RedisValue::BulkString(Bytes::from("hello")));
    }

    #[test]
    fn test_word() {
        let buf = BytesMut::from("32\r\n");
        assert_eq!(parse_word(&buf, 0), Some((4, BufRange(0, 2))));
        let buf = BytesMut::from("string\r\n");
        assert_eq!(parse_word(&buf, 0), Some((8, BufRange(0, 6))));
        let buf = BytesMut::from("32\r");
        assert!(parse_word(&buf, 0).is_none());
    }

    #[test]
    fn test_simple_string_error_succ() {
        let parsed = setup_parse(&b"+OK\r\n"[..]);
        assert_eq!(parsed, RedisValue::SimpleString("OK".into()));
        let parsed = setup_parse(&b"-Error message\r\n"[..]);
        assert_eq!(parsed, RedisValue::SimpleError("Error message".into()));
    }

    #[test]
    fn test_simple_string_error_fail() {
        let res = setup_result(&b"+OK"[..]).unwrap();
        assert!(res.is_none());
        let res = setup_result(&b"-Error"[..]).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn test_integer_succ() {
        let parsed = setup_parse(&b":0\r\n"[..]);
        assert_eq!(parsed, RedisValue::Integer(0));
        let parsed = setup_parse(&b":100\r\n"[..]);
        assert_eq!(parsed, RedisValue::Integer(100));
        let parsed = setup_parse(&b":-100\r\n"[..]);
        assert_eq!(parsed, RedisValue::Integer(-100));
    }

    #[test]
    fn test_integer_fail() {
        let res = setup_result(&b":1a0\r\n"[..]);
        assert!(res.is_err());
    }

    #[test]
    fn test_bulk_string_succ() {
        let parsed = setup_parse(&b"$-1\r\n"[..]);
        assert_eq!(parsed, RedisValue::NullBulkString);
        let parsed = setup_parse(&b"$5\r\nhello\r\n"[..]);
        assert_eq!(parsed, RedisValue::BulkString("hello".into()));
        let parsed = setup_parse(&b"$0\r\n\r\n"[..]);
        assert_eq!(parsed, RedisValue::BulkString("".into()));
    }

    #[test]
    fn test_bulk_string_fail() {
        let res = setup_result(&b"$a\r\nhellohello\r\n"[..]);
        assert!(res.is_err());
        let res = setup_result(&b"$10\r\nhello\r\n"[..]).unwrap();
        assert!(res.is_none());
        let res = setup_result(&b"$10\r\nhello678\r\n"[..]).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn test_array_succ() {
        let parsed = setup_parse(&b"*-1\r\n"[..]);
        assert_eq!(parsed, RedisValue::NullArray);
        let parsed = setup_parse(&b"*0\r\n"[..]);
        assert_eq!(parsed, RedisValue::Array(vec![]));
        let parsed = setup_parse(&b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"[..]);
        assert_eq!(
            parsed,
            RedisValue::Array(vec![
                RedisValue::BulkString("hello".into()),
                RedisValue::BulkString("world".into())
            ])
        );
        let parsed = setup_parse(&b"*3\r\n:1\r\n:2\r\n:3\r\n"[..]);
        assert_eq!(
            parsed,
            RedisValue::Array(vec![
                RedisValue::Integer(1),
                RedisValue::Integer(2),
                RedisValue::Integer(3)
            ])
        );
    }

    #[test]
    fn complex_arrays() {
        let parsed = setup_parse(&b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n"[..]);
        assert_eq!(
            parsed,
            RedisValue::Array(vec![
                RedisValue::Integer(1),
                RedisValue::Integer(2),
                RedisValue::Integer(3),
                RedisValue::Integer(4),
                RedisValue::BulkString("hello".into()),
            ])
        );

        let parsed = setup_parse(&b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n"[..]);
        assert_eq!(
            parsed,
            RedisValue::Array(vec![
                RedisValue::Array(vec![
                    RedisValue::Integer(1),
                    RedisValue::Integer(2),
                    RedisValue::Integer(3),
                ]),
                RedisValue::Array(vec![
                    RedisValue::SimpleString("Hello".into()),
                    RedisValue::SimpleError("World".into()),
                ]),
            ])
        );
    }

    #[test]
    fn test_array_fail() {
        let res = setup_result(&b"*2\r\n:1\r\n"[..]).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn test_multiple_parse() {
        let mut input = BytesMut::from(&b"+OK\r\n:100\r\n"[..]);
        let (pos, intermediate) = parse(&input, 0).unwrap().unwrap();
        let parsed = input.split_to(pos);
        assert_eq!(
            intermediate.generate_value(&parsed.freeze()),
            RedisValue::SimpleString("OK".into())
        );
        // parse the input again from index 0
        let (pos, intermediate) = parse(&input, 0).unwrap().unwrap();
        let parsed = input.split_to(pos);
        assert_eq!(
            intermediate.generate_value(&parsed.freeze()),
            RedisValue::Integer(100)
        );
    }
}
