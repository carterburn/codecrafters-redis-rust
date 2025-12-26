use anyhow::Result;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpStream, sync::mpsc::Sender};
use tokio_util::codec::Framed;

use crate::{
    command::RedisCommand,
    resp::{codec::RespFrame, RedisValue},
    server::types::{Database, ExpiryEvent, RedisKey, Value},
};

/// A type representing an active client connection
pub(crate) struct RedisConnection {
    /// Client address
    client_addr: SocketAddr,

    /// Frame to read and write data to the client
    frame: Framed<TcpStream, RespFrame>,

    /// Reference to the global key / value store
    db: Arc<Database>,
    // big question here is would it be better to have this serialized through channels? i.e. have
    // a single channel I ask for a key for...? we'll see
    //
    /// Place to send newly set keys
    expiration_tx: Sender<ExpiryEvent>,
}

impl RedisConnection {
    pub(crate) fn new(
        stream: TcpStream,
        client_addr: SocketAddr,
        db: Arc<Database>,
        expiration_tx: Sender<ExpiryEvent>,
    ) -> Self {
        Self {
            client_addr,
            frame: Framed::new(stream, RespFrame),
            db,
            expiration_tx,
        }
    }

    pub(crate) async fn client_loop(&mut self) {
        while let Some(result) = self.frame.next().await {
            match result {
                Ok(message) => {
                    tracing::info!("Received RESP value: {message:?}");
                    let cmd = match RedisCommand::parse(message) {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!("Error while parsing command: {e:?}");
                            self.send_error(e).await;
                            continue;
                        }
                    };

                    let response = match self.handle_cmd(cmd).await {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!("Error handling command: {e:?}");
                            self.send_error(e).await;
                            continue;
                        }
                    };

                    let _ = self.frame.send(response).await;
                }
                Err(e) => {
                    tracing::error!("Received error while decoding message: {e:?}");
                    self.send_error(e).await;
                    continue;
                }
            }
        }
    }

    async fn send_error(&mut self, e: anyhow::Error) {
        let _ = self
            .frame
            .send(RedisValue::SimpleError(format!("{e:?}").into()))
            .await;
    }

    async fn handle_cmd(&mut self, cmd: RedisCommand) -> Result<RedisValue> {
        match cmd {
            RedisCommand::Ping => Ok(RedisValue::SimpleString("PONG".into())),
            RedisCommand::Echo(msg) => Ok(RedisValue::BulkString(msg)),
            RedisCommand::Get(key) => match self.db.get_key(&key) {
                Some(v) => {
                    tracing::info!("Returning value: {:?}", v);
                    Ok(RedisValue::BulkString(v))
                }
                _ => Ok(RedisValue::NullBulkString),
            },
            RedisCommand::Set {
                key,
                value,
                expiration,
            } => {
                let exp = expiration.map(|dur| Instant::now() + dur);
                tracing::info!("Set {:?} -> {:?} with expiration at: {exp:?}", key, value);

                let val = Value::new(value, exp);
                self.db.set_key(&key, val);
                // send our new expiration time to the channel if needed
                if let Some(time) = exp {
                    let _ = self.expiration_tx.send((time, key)).await;
                };
                Ok(RedisValue::SimpleString("OK".into()))
            }
            RedisCommand::RPush {
                list_name,
                elements,
            } => {
                tracing::info!("RPush to {list_name:?} with elements: {elements:?}");
                let size = self.db.rpush(
                    &list_name,
                    elements.iter().map(|e| Value::new(e.clone(), None)),
                );
                Ok(RedisValue::Integer(size as i64))
            }
        }
    }
}
