// Run a SQL query against data stored in a CSV

use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Register the table
    let ctx = SessionContext::new();
    ctx.register_csv("example", "../common/data/example.csv", CsvReadOptions::new()).await?;

    // Create a plan to run a SQL query
    let df = ctx.sql("SELECT a, MIN(b) FROM example WHERE a <= b GROUP BY a LIMIT 100").await?;

    // Execute and print results
    df.show().await?;
    Ok(())
}
