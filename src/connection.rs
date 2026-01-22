use anyhow::Result;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{self, Sender},
        oneshot::{self, Receiver},
    },
};
use tokio_util::codec::Framed;

use crate::{
    command::{ExecutorCommand, ExecutorResponse, RedisCommand},
    resp::{codec::RespFrame, RespValue},
    server::{
        database::Database,
        types::{ExpiryEvent, RedisDataType, StoredValue},
    },
};

/// A type representing an active client connection
pub(crate) struct RedisConnection {
    /// Frame to read and write data to the client
    frame: Framed<TcpStream, RespFrame>,

    /// channel to the executor task
    executor_tx: mpsc::Sender<ExecutorCommand>,
    // big question here is would it be better to have this serialized through channels? i.e. have
    // a single channel I ask for a key for...? we'll see
    //
    // Place to send newly set keys
    // TODO: fix expiration_tx: Sender<ExpiryEvent>,
}

impl RedisConnection {
    pub(crate) fn new(stream: TcpStream, executor_tx: mpsc::Sender<ExecutorCommand>) -> Self {
        Self {
            frame: Framed::new(stream, RespFrame),
            executor_tx,
        }
    }

    pub(crate) async fn client_loop(&mut self) {
        let mut transaction: Option<Vec<RedisCommand>> = None;
        while let Some(result) = self.frame.next().await {
            match result {
                Ok(message) => {
                    tracing::info!("Received RESP value: {message:?}");
                    let mut cmd = match RedisCommand::parse(message) {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!("Error while parsing command: {e:?}");
                            self.send_error(e).await;
                            continue;
                        }
                    };

                    // check if these are transaction based commands, because we take certain
                    // actions if that's the case
                    if let RedisCommand::Multi = cmd {
                        if transaction.is_some() {
                            // error case: multi inside a multi is not possible
                            self.send_error(anyhow::anyhow!("Duplicate MULTI commands"))
                                .await;
                        } else {
                            // start up the transaction tracking
                            transaction = Some(vec![]);
                            let _ = self.frame.send(RespValue::SimpleString("OK".into())).await;
                        }
                        continue;
                    }

                    if let RedisCommand::Discard = cmd {
                        // abort the transaction iff a transaction was started
                        let ret = match transaction.take() {
                            Some(_c) => RespValue::SimpleString("OK".into()),
                            None => RespValue::SimpleError("ERR DISCARD without MULTI".into()),
                        };
                        let _ = self.frame.send(ret).await;
                        continue;
                    }

                    if let RedisCommand::Exec = cmd {
                        let Some(cmds) = transaction.take() else {
                            // no transaction started, error
                            self.send_error(anyhow::anyhow!("ERR EXEC without MULTI"))
                                .await;
                            continue;
                        };

                        // everything is properly setup for transaction, so replace cmd with the
                        // transaction command
                        cmd = RedisCommand::Transaction { commands: cmds };
                        // this continues on down to send to the executor
                    }

                    if let Some(ref mut cmds) = transaction {
                        // currently in a transaction so queue up this command and continue looping
                        cmds.push(cmd);
                        let _ = self
                            .frame
                            .send(RespValue::SimpleString("QUEUED".into()))
                            .await;
                        continue;
                    }

                    let (tx, rx) = oneshot::channel();
                    if let Err(e) = self
                        .executor_tx
                        .send(ExecutorCommand {
                            command: cmd,
                            respond_to: tx,
                        })
                        .await
                    {
                        tracing::error!("Error sending command to executor: {e:?}");
                        break;
                    }

                    // listen for response to send back
                    let Ok(response) = rx.await else {
                        tracing::error!("Error receiving response");
                        continue;
                    };

                    match response {
                        ExecutorResponse::Value(v) => {
                            let _ = self.frame.send(v).await;
                        }
                        ExecutorResponse::Blocking { rx, key, timeout } => {
                            let block_response = self.block(rx, key, timeout).await;
                            let _ = self.frame.send(block_response).await;
                        }
                        ExecutorResponse::XReadBlock { rx, timeout } => {
                            let block_response = self.xread_block(rx, timeout).await;
                            let _ = self.frame.send(block_response).await;
                        }
                    }
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
            .send(RespValue::SimpleError(format!("{e:?}").into()))
            .await;
    }

    async fn block(&mut self, rx: Receiver<Bytes>, key: Bytes, timeout: f64) -> RespValue {
        tokio::select! {
            _ = async {
                if timeout == 0.0 {
                    tracing::info!("Indefinite wait for {key:?}");
                    std::future::pending::<()>().await
                } else {
                    tracing::info!("Waiting for {timeout} on {key:?}");
                    tokio::time::sleep_until(tokio::time::Instant::from_std(Instant::now() + Duration::from_secs_f64(timeout))).await
                }
            } => {
                // timeout reached!
                RespValue::NullArray
            },
            Ok(value) = rx => {
                RespValue::Array(vec![RespValue::BulkString(key.clone()), RespValue::BulkString(value.clone())])
            }
        }
    }

    async fn xread_block(&mut self, mut rx: mpsc::Receiver<RespValue>, timeout: u64) -> RespValue {
        tokio::select! {
            _ = async {
                if timeout == 0 {
                    tracing::info!("Indefinite wait on XRead for key");
                    std::future::pending::<()>().await
                } else {
                    tracing::info!("Waiting for {timeout}");
                    tokio::time::sleep_until(tokio::time::Instant::from_std(Instant::now() + Duration::from_millis(timeout))).await
                }
            } => {
                RespValue::NullArray
            },
            Some(value) = rx.recv() => {
                value
            }
        }
    }
}
