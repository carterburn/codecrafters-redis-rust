use anyhow::Result;
use codecrafters_redis::Redis;

const REDIS_PORT: u16 = 6379;

#[tokio::main]
async fn main() -> Result<()> {
    let mut redis = Redis::new(REDIS_PORT).await?;

    redis.run().await?;

    Ok(())
}
