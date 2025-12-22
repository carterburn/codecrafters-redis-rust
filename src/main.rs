use anyhow::Result;
use codecrafters_redis::server::Redis;

const REDIS_PORT: u16 = 6379;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut redis = Redis::new(REDIS_PORT).await?;

    redis.run().await?;

    Ok(())
}
