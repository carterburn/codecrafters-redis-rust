use crate::resp::RespValue;

use std::{num::ParseIntError, str::Utf8Error};

use nom::{
    branch::alt,
    bytes::streaming::{tag, take, take_until},
    character::streaming::crlf,
    combinator::map_res,
    error::{ErrorKind, FromExternalError, ParseError},
    multi::many_m_n,
    sequence::terminated,
    IResult, Parser,
};

#[derive(Debug, PartialEq)]
pub(crate) enum RespParseError<I> {
    ParseUtf8Error(Utf8Error),
    ParseIntegerError(ParseIntError),
    NomErr(I, ErrorKind),
}

impl<I> ParseError<I> for RespParseError<I> {
    fn from_error_kind(input: I, kind: ErrorKind) -> Self {
        RespParseError::NomErr(input, kind)
    }

    fn append(_input: I, _kind: ErrorKind, other: Self) -> Self {
        other
    }
}

impl<I, E> FromExternalError<I, E> for RespParseError<I>
where
    E: Into<RespParseError<I>>,
{
    fn from_external_error(_input: I, _kind: ErrorKind, e: E) -> Self {
        e.into()
    }
}

impl<I> From<Utf8Error> for RespParseError<I> {
    fn from(value: Utf8Error) -> Self {
        RespParseError::ParseUtf8Error(value)
    }
}

impl<I> From<ParseIntError> for RespParseError<I> {
    fn from(value: ParseIntError) -> Self {
        RespParseError::ParseIntegerError(value)
    }
}

type RespParseResult<I, O, E = RespParseError<I>> = IResult<I, O, E>;

fn parse_simple(input: &[u8]) -> RespParseResult<&[u8], RespValue> {
    let tpl = (
        alt((tag(&b"+"[..]), tag(&b"-"[..]))),
        take_until("\r\n"),
        crlf,
    );
    map_res(tpl, |parsed: (&[u8], &[u8], &[u8])| {
        // save ourselves the heap allocation until we know that this is valid utf-8
        let s = str::from_utf8(parsed.1)?;
        if parsed.0 == &b"+"[..] {
            Ok::<RespValue, RespParseError<&[u8]>>(RespValue::SimpleString(s.to_string()))
        } else {
            Ok(RespValue::SimpleError(s.to_string()))
        }
    })
    .parse(input)
}

fn int_word(input: &[u8]) -> RespParseResult<&[u8], i64> {
    map_res((take_until("\r\n"), crlf), |parsed: (&[u8], &[u8])| {
        let num_str = str::from_utf8(parsed.0)?;
        Ok::<i64, RespParseError<&[u8]>>(num_str.parse()?)
    })
    .parse(input)
}

fn parse_integer(input: &[u8]) -> RespParseResult<&[u8], RespValue> {
    let tpl = (tag(&b":"[..]), int_word);
    map_res(tpl, |parsed: (&[u8], i64)| {
        Ok::<RespValue, RespParseError<&[u8]>>(RespValue::Integer(parsed.1))
    })
    .parse(input)
}

fn parse_bulk_string(input: &[u8]) -> RespParseResult<&[u8], RespValue> {
    let (input, (_, s_len)) = (tag(&b"$"[..]), int_word).parse(input)?;

    // check for Null string
    if s_len < 0 {
        return Ok((input, RespValue::NullBulkString));
    }

    // now we can take as many bytes as needed based on s_len
    map_res(terminated(take(s_len as usize), crlf), |parsed| {
        let s = str::from_utf8(parsed)?;
        Ok::<RespValue, RespParseError<&[u8]>>(RespValue::BulkString(s.to_string()))
    })
    .parse(input)
}

fn parse_array(input: &[u8]) -> RespParseResult<&[u8], RespValue> {
    // first grab the number of elements information (don't forget null arrays!)
    let (input, (_, a_len)) = (tag(&b"*"[..]), int_word).parse(input)?;

    // check for Null array
    if a_len < 0 {
        return Ok((input, RespValue::NullArray));
    }

    // now we parse as many RespValue's as we can and try to collect them into a Vec of RespValues
    map_res(
        many_m_n(a_len as usize, a_len as usize, parse_resp),
        |parsed: Vec<RespValue>| Ok::<RespValue, RespParseError<&[u8]>>(RespValue::Array(parsed)),
    )
    .parse(input)
}

pub(crate) fn parse_resp(input: &[u8]) -> RespParseResult<&[u8], RespValue> {
    let mut p = alt([parse_simple, parse_integer, parse_bulk_string, parse_array]);
    p.parse(input)
}

#[cfg(test)]
mod tests {
    use std::num::NonZero;

    use super::*;

    #[test]
    fn test_simple_string_error_succ() {
        let (_remain, parsed) = parse_resp(&b"+OK\r\n"[..]).unwrap();
        assert_eq!(parsed, RespValue::SimpleString("OK".to_string()));
        let (_remain, parsed) = parse_resp(&b"-Error message\r\n"[..]).unwrap();
        assert_eq!(parsed, RespValue::SimpleError("Error message".to_string()));
    }

    #[test]
    fn test_simple_string_error_fail() {
        let res = parse_resp(&b"+OK"[..]);
        assert_eq!(res, Err(nom::Err::Incomplete(nom::Needed::Unknown)));
        let res = parse_resp(&b"-Error"[..]);
        assert_eq!(res, Err(nom::Err::Incomplete(nom::Needed::Unknown)));
        let non_utf8_bytes: Vec<u8> = vec![b'+', 0xa9, 0xfe, 0xff, b'\r', b'\n'];
        let res = parse_resp(&non_utf8_bytes);
        assert!(res.is_err());
    }

    #[test]
    fn test_integer_succ() {
        let (_, parsed) = parse_resp(&b":0\r\n"[..]).unwrap();
        assert_eq!(parsed, RespValue::Integer(0));
        let (_, parsed) = parse_resp(&b":100\r\n"[..]).unwrap();
        assert_eq!(parsed, RespValue::Integer(100));
        let (_, parsed) = parse_resp(&b":-100\r\n"[..]).unwrap();
        assert_eq!(parsed, RespValue::Integer(-100));
    }

    #[test]
    fn test_integer_fail() {
        let res = parse_resp(&b":1a0\r\n"[..]);
        assert!(res.is_err());
    }

    #[test]
    fn test_bulk_string_succ() {
        let (_, parsed) = parse_resp(&b"$-1\r\n"[..]).unwrap();
        assert_eq!(parsed, RespValue::NullBulkString);
        let (_, parsed) = parse_resp(&b"$5\r\nhello\r\n"[..]).unwrap();
        assert_eq!(parsed, RespValue::BulkString("hello".to_string()));
        let (_, parsed) = parse_resp(&b"$0\r\n\r\n"[..]).unwrap();
        assert_eq!(parsed, RespValue::BulkString("".to_string()));
    }

    #[test]
    fn test_bulk_string_fail() {
        let res = parse_resp(&b"$a\r\nhellohello\r\n"[..]);
        assert!(res.is_err());
        let res = parse_resp(&b"$10\r\nhello\r\n"[..]);
        // NOTE: The size is really 5, but the \r\n is counted greedily until it's needed to fully
        // complete the value; see example below where 2 bytes are still needed. this says we need
        // 3 total bytes to go, but we really need 3 + 2 for the \r\n. if we fully parse the length
        //   of the string, we will still need the final 2 for \r\n
        assert_eq!(
            res,
            Err(nom::Err::Incomplete(nom::Needed::Size(
                NonZero::new(3).unwrap()
            )))
        );
        let res = parse_resp(&b"$10\r\nhello678\r\n"[..]);
        assert_eq!(
            res,
            Err(nom::Err::Incomplete(nom::Needed::Size(
                NonZero::new(2).unwrap()
            )))
        );
    }

    #[test]
    fn test_array_succ() {
        let (_, parsed) = parse_resp(&b"*-1\r\n"[..]).unwrap();
        assert_eq!(parsed, RespValue::NullArray);
        let (_, parsed) = parse_resp(&b"*0\r\n"[..]).unwrap();
        assert_eq!(parsed, RespValue::Array(vec![]));
        let (_, parsed) = parse_resp(&b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"[..]).unwrap();
        assert_eq!(
            parsed,
            RespValue::Array(vec![
                RespValue::BulkString("hello".to_string()),
                RespValue::BulkString("world".to_string())
            ])
        );
        let (_, parsed) = parse_resp(&b"*3\r\n:1\r\n:2\r\n:3\r\n"[..]).unwrap();
        assert_eq!(
            parsed,
            RespValue::Array(vec![
                RespValue::Integer(1),
                RespValue::Integer(2),
                RespValue::Integer(3)
            ])
        );
    }

    #[test]
    fn complex_arrays() {
        let (_, parsed) =
            parse_resp(&b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n"[..]).unwrap();
        assert_eq!(
            parsed,
            RespValue::Array(vec![
                RespValue::Integer(1),
                RespValue::Integer(2),
                RespValue::Integer(3),
                RespValue::Integer(4),
                RespValue::BulkString("hello".to_string()),
            ])
        );

        let (_, parsed) =
            parse_resp(&b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n"[..]).unwrap();
        assert_eq!(
            parsed,
            RespValue::Array(vec![
                RespValue::Array(vec![
                    RespValue::Integer(1),
                    RespValue::Integer(2),
                    RespValue::Integer(3),
                ]),
                RespValue::Array(vec![
                    RespValue::SimpleString("Hello".to_string()),
                    RespValue::SimpleError("World".to_string()),
                ]),
            ])
        );
    }

    #[test]
    fn test_array_fail() {
        // will still need 1 RespValue for this array (the size needed is from the many_m_n
        // combinator)
        let res = parse_resp(&b"*2\r\n:1\r\n"[..]);
        assert_eq!(
            res,
            Err(nom::Err::Incomplete(nom::Needed::Size(
                NonZero::new(1).unwrap()
            )))
        );
        assert_eq!(
            res,
            Err(nom::Err::Incomplete(nom::Needed::Size(
                NonZero::new(1).unwrap()
            )))
        );
    }

    #[test]
    fn test_multiple_parse() {
        let input = b"+OK\r\n:100\r\n";
        let (remain, parsed) = parse_resp(&input[..]).unwrap();
        assert_eq!(parsed, RespValue::SimpleString("OK".to_string()));
        let (remain, parsed) = parse_resp(remain).unwrap();
        assert_eq!(parsed, RespValue::Integer(100));
        assert!(remain.is_empty());
    }
}
