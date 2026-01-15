use std::{
    collections::{BTreeMap, VecDeque},
    time::Instant,
};

use anyhow::Result;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use memchr::memchr;

pub(crate) type RedisKey = Bytes;

#[derive(Clone, Debug)]
pub enum RedisDataType {
    String(Bytes),
    List(VecDeque<Bytes>),
    Stream(BTreeMap<EntryId, Vec<Bytes>>),
}

#[derive(Clone, Debug)]
pub(crate) struct StoredValue {
    /// The actual value
    pub value: RedisDataType,

    /// Last set time (if key was set with expirations)
    expiration: Option<Instant>,
}

impl StoredValue {
    pub(crate) fn new(value: RedisDataType, expiration: Option<Instant>) -> Self {
        Self { value, expiration }
    }

    pub(crate) fn expired(&self, current: Instant) -> bool {
        if let Some(expiration) = self.expiration {
            if current >= expiration {
                // key is now expired
                true
            } else {
                // key still valid
                false
            }
        } else {
            // Not expired because it can't
            false
        }
    }

    pub(crate) fn get_value(&self) -> &RedisDataType {
        &self.value
    }

    pub(crate) fn get_expiration(&self) -> Option<&Instant> {
        self.expiration.as_ref()
    }

    pub(crate) fn as_string(&self) -> Option<Bytes> {
        match &self.value {
            RedisDataType::String(s) => Some(s.slice(..)),
            _ => None,
        }
    }

    pub(crate) fn as_list(&self) -> Option<&VecDeque<Bytes>> {
        match &self.value {
            RedisDataType::List(l) => Some(l),
            _ => None,
        }
    }

    pub(crate) fn as_list_mut(&mut self) -> Option<&mut VecDeque<Bytes>> {
        match &mut self.value {
            RedisDataType::List(l) => Some(l),
            _ => None,
        }
    }

    pub(crate) fn as_stream(&self) -> Option<&BTreeMap<EntryId, Vec<Bytes>>> {
        match &self.value {
            RedisDataType::Stream(s) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn as_stream_mut(&mut self) -> Option<&mut BTreeMap<EntryId, Vec<Bytes>>> {
        match &mut self.value {
            RedisDataType::Stream(s) => Some(s),
            _ => None,
        }
    }
}

enum EntryIdFormat<'a> {
    Star,
    TimeStar { time: &'a str },
    TimeSeq { time: &'a str, seq: &'a str },
}

impl<'a> EntryIdFormat<'a> {
    fn parse(value: &'a Bytes) -> Result<Self> {
        if value == &b"*"[..] {
            return Ok(Self::Star);
        }

        match memchr(b'-', &value[..]) {
            Some(dash) => {
                if dash == value.len() - 1 {
                    // nothing else after the dash
                    return Err(anyhow::anyhow!("Invalid format for Entry ID"));
                }

                let time = str::from_utf8(&value[..dash])?;
                let seq = str::from_utf8(&value[dash + 1..])?;
                if seq == "*" {
                    Ok(Self::TimeStar { time })
                } else {
                    Ok(Self::TimeSeq { time, seq })
                }
            }
            None => Err(anyhow::anyhow!("Invalid format for Entry ID")),
        }
    }
}

/// EntryId for Streams
/// Ord is derived in the order of the struct's fields, so milli_time must be first to compare the
/// millisecond time then the sequence number if the millisecond times are equal.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct EntryId {
    /// The first portion of the Entry ID
    milli_time: DateTime<Utc>,

    /// Sequence number after the milli_time
    seq: usize,
}

impl EntryId {
    pub(crate) fn construct(
        entry_id: &Bytes,
        last_kv: Option<(&Self, &Vec<Bytes>)>,
    ) -> Result<Self, anyhow::Error> {
        let last_key_check = |proposal: &DateTime<Utc>, last_key: &EntryId, default: usize| {
            if *proposal < last_key.milli_time {
                Err(anyhow::anyhow!("ERR The ID specified in XADD is equal or smaller than the target stream top item"))
            } else if *proposal == last_key.milli_time {
                Ok(last_key.seq + 1)
            } else {
                Ok(default)
            }
        };
        let format = EntryIdFormat::parse(entry_id)?;

        match format {
            EntryIdFormat::Star => {
                // use current timestamp when auto generating IDs
                let milli_time = Utc::now();
                let seq = match last_kv {
                    Some((last_key, _)) => last_key_check(&milli_time, last_key, 0)?,
                    None => 0,
                };

                Ok(Self { milli_time, seq })
            }
            EntryIdFormat::TimeStar { time } => {
                // specified time from user, we will generate the sequence number
                // special case: if time == 0
                if time == "0" {
                    let milli_time = DateTime::from_timestamp_millis(0)
                        .ok_or(anyhow::anyhow!("Time creation failed"))?;
                    let seq = match last_kv {
                        Some((last_key, _)) => last_key_check(&milli_time, last_key, 1)?,
                        None => 1,
                    };
                    Ok(Self { milli_time, seq })
                } else {
                    let time_millis: i64 = time.parse()?;
                    let milli_time = DateTime::from_timestamp_millis(time_millis)
                        .ok_or(anyhow::anyhow!("Time creation failed: Invalid time"))?;
                    let seq = match last_kv {
                        Some((last_key, _)) => last_key_check(&milli_time, last_key, 0)?,
                        None => 0,
                    };
                    Ok(Self { milli_time, seq })
                }
            }
            EntryIdFormat::TimeSeq { time, seq } => {
                // before doing any logic, check if we have a 0-0 case
                if time == "0" && seq == "0" {
                    return Err(anyhow::anyhow!(
                        "ERR The ID specified in XADD must be greater than 0-0"
                    ));
                }
                // user specified everything
                match last_kv {
                    Some((last_key, _)) => {
                        // ensure time is >= last entry's millisecond time
                        let time_millis: i64 = time.parse()?;
                        let seq: usize = seq.parse()?;

                        let milli_time = DateTime::from_timestamp_millis(time_millis)
                            .ok_or(anyhow::anyhow!("Time creation failed: invalid time"))?;
                        if milli_time < last_key.milli_time {
                            return Err(anyhow::anyhow!("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
                        } else if milli_time == last_key.milli_time {
                            // seq has to be greater than last_key's seq
                            if seq <= last_key.seq {
                                return Err(anyhow::anyhow!("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
                            }
                        }

                        // if we didn't send out the errors, were ok
                        Ok(Self { milli_time, seq })
                    }
                    None => {
                        // stream empty, just needs to be greater than 0-0
                        let time_millis: i64 = time.parse()?;
                        let seq: usize = seq.parse()?;

                        let milli_time = DateTime::from_timestamp_millis(time_millis)
                            .ok_or(anyhow::anyhow!("Time creation failed: invalid time"))?;

                        Ok(Self { milli_time, seq })
                    }
                }
            }
        }
    }

    pub(crate) fn parse_range(value: &Bytes, lower: bool) -> Result<EntryId> {
        // attempt to parse a full EntryId in the format: <millisecondTime>-<sequenceNumber>
        // if no dash, use lower to determine sequence number for comparison
        match memchr(b'-', value) {
            Some(dash) => {
                if dash == value.len() - 1 {
                    return Err(anyhow::anyhow!("Improper value format for EntryID"));
                }
                let milli_time =
                    DateTime::from_timestamp_millis(str::from_utf8(&value[..dash])?.parse()?)
                        .ok_or(anyhow::anyhow!("Invalid millisecondTime"))?;
                let seq = str::from_utf8(&value[dash + 1..])?.parse()?;
                Ok(Self { milli_time, seq })
            }
            None => {
                // only have the millisecond time
                let milli_time =
                    DateTime::from_timestamp_millis(str::from_utf8(&value[..])?.parse()?)
                        .ok_or(anyhow::anyhow!("Invalid millisecondTime"))?;

                let seq = if lower { 0 } else { usize::MAX };

                Ok(Self { milli_time, seq })
            }
        }
    }
}

impl std::fmt::Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}-{}", self.milli_time.timestamp_millis(), self.seq)
    }
}

pub(crate) type ExpiryEvent = (Instant, RedisKey);
