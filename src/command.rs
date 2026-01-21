use core::str;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::{
    resp::RespValue,
    server::{
        database::Database,
        types::{EntryId, RedisDataType, StoredValue},
    },
};

pub(crate) struct ExecutorCommand {
    pub command: RedisCommand,

    pub respond_to: oneshot::Sender<ExecutorResponse>,
}

pub(crate) enum ExecutorResponse {
    Value(RespValue),
    Blocking {
        rx: oneshot::Receiver<Bytes>,
        key: Bytes,
        timeout: f64,
    },
    XReadBlock {
        rx: mpsc::Receiver<RespValue>,
        timeout: u64,
    },
}

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
    LPush {
        list_name: Bytes,
        elements: Vec<Bytes>,
    },
    LRange {
        list_name: Bytes,
        start: isize,
        end: isize,
    },
    LLen {
        list_name: Bytes,
    },
    LPop {
        list_name: Bytes,
        num_pops: Option<usize>,
    },
    BLPop {
        list_name: Bytes,
        timeout: f64,
    },
    Type {
        key_name: Bytes,
    },
    XAdd {
        stream_key: Bytes,
        entry_id: Bytes,
        pairs: Vec<Bytes>,
    },
    XRange {
        stream_key: Bytes,
        start: Bytes,
        end: Bytes,
    },
    XRead {
        streams: Vec<Bytes>,
        timeout: Option<usize>,
    },
}

impl RedisCommand {
    pub(crate) fn parse(msg: RespValue) -> Result<Self> {
        // ensure that RespValue is a BulkArray
        let RespValue::Array(values) = msg else {
            tracing::error!("Invalid message, expected bulk array");
            Err(anyhow::anyhow!("Invalid message"))?
        };

        let cmd = values
            .first()
            .and_then(|v| match v {
                RespValue::BulkString(s) => {
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
            "LPUSH" => {
                let list_name = Self::expect_bulk_string(&values, 1)?;
                let elements: Result<Vec<Bytes>, anyhow::Error> =
                    values[2..].iter().map(|rv| rv.try_into()).collect();
                Ok(Self::LPush {
                    list_name,
                    elements: elements?,
                })
            }
            "LRANGE" => {
                let list_name = Self::expect_bulk_string(&values, 1)?;
                // expect two more bulk strings, otherwise, it is an error
                let start = Self::expect_bulk_string(&values, 2)?;
                let end = Self::expect_bulk_string(&values, 3)?;
                let start = str::from_utf8(&start)?;
                let end = str::from_utf8(&end)?;
                let start: isize = start.parse()?;
                let end: isize = end.parse()?;

                Ok(Self::LRange {
                    list_name,
                    start,
                    end,
                })
            }
            "LLEN" => {
                let list_name = Self::expect_bulk_string(&values, 1)?;
                Ok(Self::LLen { list_name })
            }
            "LPOP" => {
                let list_name = Self::expect_bulk_string(&values, 1)?;

                let num_pops: Option<usize> = if values.len() > 2 {
                    let num_pops_str: String = values[2].clone().try_into()?;
                    Some(num_pops_str.parse()?)
                } else {
                    None
                };

                Ok(Self::LPop {
                    list_name,
                    num_pops,
                })
            }
            "BLPOP" => {
                let list_name = Self::expect_bulk_string(&values, 1)?;
                let timeout = Self::expect_bulk_string(&values, 2)?;
                let timeout: &str = str::from_utf8(&timeout[..])?;
                let timeout: f64 = timeout.parse()?;

                Ok(Self::BLPop { list_name, timeout })
            }
            "TYPE" => {
                let key_name = Self::expect_bulk_string(&values, 1)?;
                Ok(Self::Type { key_name })
            }
            "XADD" => {
                let stream_key = Self::expect_bulk_string(&values, 1)?;
                let entry_id = Self::expect_bulk_string(&values, 2)?;

                let pairs: Vec<Bytes> = values[3..]
                    .iter()
                    .filter_map(|rv| match rv {
                        RespValue::BulkString(b) => Some(b.slice(..)),
                        _ => None,
                    })
                    .collect();

                Ok(Self::XAdd {
                    stream_key,
                    entry_id,
                    pairs,
                })
            }
            "XRANGE" => {
                let stream_key = Self::expect_bulk_string(&values, 1)?;
                let start = Self::expect_bulk_string(&values, 2)?;
                let end = Self::expect_bulk_string(&values, 3)?;

                Ok(Self::XRange {
                    stream_key,
                    start,
                    end,
                })
            }
            "XREAD" => {
                let opt = Self::expect_bulk_string(&values, 1)?;
                let opt = str::from_utf8(&opt[..])?;
                let timeout = match opt.to_uppercase().as_str() {
                    "BLOCK" => {
                        let value = Self::expect_bulk_string(&values, 2)?;
                        let value: usize = str::from_utf8(&value[..])?.parse()?;
                        Some(value)
                    }
                    "STREAMS" => None,
                    _ => {
                        return Err(anyhow::anyhow!("Missing BLOCK or STREAMS after XREAD"));
                    }
                };

                let rest = if timeout.is_some() {
                    let stream_word = Self::expect_bulk_string(&values, 3)?;
                    let stream_word = str::from_utf8(&stream_word[..])?;
                    if stream_word.to_uppercase().as_str() != "STREAMS" {
                        return Err(anyhow::anyhow!("Missing STREAMS in XREAD command"));
                    }
                    &values[4..]
                } else {
                    &values[2..]
                };

                let streams: Vec<Bytes> = rest
                    .iter()
                    .filter_map(|v| match v {
                        RespValue::BulkString(b) => Some(b.slice(..)),
                        _ => None,
                    })
                    .collect();

                if !streams.len().is_multiple_of(2) {
                    return Err(anyhow::anyhow!("Not enough streams and Entry IDs"));
                }

                Ok(Self::XRead { streams, timeout })
            }
            _ => Err(anyhow::anyhow!("Unsupported command: {cmd:?}")),
        }
    }

    fn expect_bulk_string(values: &[RespValue], index: usize) -> Result<Bytes> {
        values
            .get(index)
            .and_then(|redis_val| match redis_val {
                RespValue::BulkString(b) => Some(b.slice(..)),
                _ => None,
            })
            .ok_or(anyhow::anyhow!(
                "Expected bulk string at index {index} of {values:?}"
            ))
    }
}

fn process_time<F>(dur: &RespValue, f: F) -> Result<Duration>
where
    F: Fn(u64) -> Duration,
{
    let dur_str: String = dur.try_into()?;
    let dur: u64 = dur_str.parse()?;
    Ok(f(dur))
}
