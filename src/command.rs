use core::str;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;

use crate::resp::RedisValue;

pub(crate) enum RedisCommand {
    Ping,
    Echo(Bytes),
    Get(Bytes),
    Set {
        key: Bytes,
        value: Bytes,
        expiration: Option<Duration>,
    },
    RPush {
        list_name: Bytes,
        elements: Vec<Bytes>,
    },
}

impl RedisCommand {
    pub(crate) fn parse(msg: RedisValue) -> Result<Self> {
        // ensure that RedisValue is a BulkArray
        let RedisValue::Array(values) = msg else {
            tracing::error!("Invalid message, expected bulk array");
            Err(anyhow::anyhow!("Invalid message"))?
        };

        let cmd = values
            .first()
            .and_then(|v| match v {
                RedisValue::BulkString(s) => {
                    // attempt to interpret as String
                    let s = str::from_utf8(&s[..]).ok()?;
                    Some(s.to_uppercase())
                }
                _ => None,
            })
            .ok_or(anyhow::anyhow!("Invalid type in command array"))?;

        match cmd.as_str() {
            "PING" => Ok(Self::Ping),
            "ECHO" => {
                let msg = Self::expect_bulk_string(&values, 1)?;
                Ok(Self::Echo(msg))
            }
            "GET" => {
                let key = Self::expect_bulk_string(&values, 1)?;
                Ok(Self::Get(key))
            }
            "SET" => {
                // set requires key and value
                let key = Self::expect_bulk_string(&values, 1)?;
                let value = Self::expect_bulk_string(&values, 2)?;

                let mut expiration = None;

                let mut rest = values[3..].iter();
                while let Some(v) = rest.next() {
                    let arg: String = v.try_into()?;
                    match arg.as_str() {
                        "PX" => {
                            let dur = rest.next().ok_or(anyhow::anyhow!(
                                "Not enough args, expected duration specifier"
                            ))?;
                            expiration = Some(process_time(dur, Duration::from_millis)?);
                        }
                        "EX" => {
                            let dur = rest.next().ok_or(anyhow::anyhow!(
                                "Not enough args, expected duration specifier"
                            ))?;
                            expiration = Some(process_time(dur, Duration::from_secs)?);
                        }
                        _ => {
                            return Err(anyhow::anyhow!("Unsupported or invalid argument: {arg}"));
                        }
                    }
                }

                Ok(Self::Set {
                    key,
                    value,
                    expiration,
                })
            }
            "RPUSH" => {
                let list_name = Self::expect_bulk_string(&values, 1)?;
                // collect remaining values as Bytes values
                let elements: Result<Vec<Bytes>, anyhow::Error> =
                    values[2..].iter().map(|rv| rv.try_into()).collect();
                Ok(Self::RPush {
                    list_name,
                    elements: elements?,
                })
            }
            _ => Err(anyhow::anyhow!("Unsupported command: {cmd:?}")),
        }
    }

    fn expect_bulk_string(values: &[RedisValue], index: usize) -> Result<Bytes> {
        values
            .get(index)
            .and_then(|redis_val| match redis_val {
                RedisValue::BulkString(b) => Some(b.slice(..)),
                _ => None,
            })
            .ok_or(anyhow::anyhow!(
                "Expected bulk string at index {index} of {values:?}"
            ))
    }
}

fn process_time<F>(dur: &RedisValue, f: F) -> Result<Duration>
where
    F: Fn(u64) -> Duration,
{
    let dur_str: String = dur.try_into()?;
    let dur: u64 = dur_str.parse()?;
    Ok(f(dur))
}
