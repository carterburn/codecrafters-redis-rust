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

                // optional expiration
                let expiration = if values.len() >= 5 {
                    // SET key value [PX|EX] <duration>
                    let exp_type = Self::expect_bulk_string(&values, 3)?;
                    let exp_dur = Self::expect_bulk_string(&values, 4)?;

                    let dur: u64 = str::from_utf8(&exp_dur[..])?.parse()?;
                    let ex_type = str::from_utf8(&exp_type[..])?;
                    match ex_type.to_uppercase().as_str() {
                        "PX" => Some(Duration::from_millis(dur)),
                        "EX" => Some(Duration::from_secs(dur)),
                        _ => {
                            return Err(anyhow::anyhow!("Invalid expiration type: {ex_type}"));
                        }
                    }
                } else {
                    None
                };

                Ok(Self::Set {
                    key,
                    value,
                    expiration,
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
