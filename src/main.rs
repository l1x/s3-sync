use anyhow::Result;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

mod cli;
mod s3;
mod progress;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the logger
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    // Parse command line arguments
    let args = cli::parse_args();

    // Process arguments into a sync configuration
    let sync_config = match cli::process_sync_args(&args) {
        Ok(config) => config,
        Err(e) => {
            error!("Error processing arguments: {}", e);
            return Err(e);
        }
    };

    info!("Starting S3 sync operation");

    // Run the sync operation
    match s3::run_sync(sync_config).await {
        Ok(_) => {
            info!("Sync completed successfully");
            Ok(())
        }
        Err(e) => {
            error!("Sync failed: {}", e);
            Err(e)
        }
    }
}
