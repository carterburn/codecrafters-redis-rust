use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

pub struct Redis {
    /// TCP Listener on given port
    listener: TcpListener,
}

impl Redis {
    pub async fn new(port: u16) -> Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(("127.0.0.1", port)).await?,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        println!("Serving clients");
        while let Ok((mut client_stream, client_addr)) = self.listener.accept().await {
            println!("New connection from: {client_addr}");

            tokio::spawn(async move {
                // Here, we just hardcode our response, but future we will likely want to make a
                // new struct that represents a client connection to handle that
                loop {
                    let mut buf = [0u8; 4096];
                    let n = client_stream.read(&mut buf).await?;
                    println!(
                        "Read {n} bytes from client: {:?}",
                        str::from_utf8(&buf[..n])
                    );
                    client_stream.write_all(b"+PONG\r\n").await?;
                }
                #[allow(unreachable_code)]
                Ok::<(), anyhow::Error>(())
            });
        }
        Ok(())
    }
}
