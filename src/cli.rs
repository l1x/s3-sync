use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Command line arguments for the S3 sync tool
#[derive(Parser, Debug, Clone)]
#[clap(
    name = "s3sync",
    about = "A multithreaded S3 sync tool written in Rust"
)]
pub struct Args {
    /// AWS region
    #[clap(short, long, default_value = "us-east-1")]
    pub region: String,

    /// Number of concurrent operations
    #[clap(short, long, default_value = "10")]
    pub concurrency: usize,

    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Sync files between S3 and local filesystem
    Sync {
        /// Source location (can be local path or s3://bucket/prefix)
        source: String,

        /// Destination location (can be local path or s3://bucket/prefix)
        destination: String,
    },
}

#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub source_type: LocationType,
    pub dest_type: LocationType,
    pub s3_bucket: Option<String>,
    pub s3_prefix: Option<String>,
    pub local_path: Option<PathBuf>,
    pub dest_s3_bucket: Option<String>,
    pub dest_s3_prefix: Option<String>,
    pub dest_local_path: Option<PathBuf>,
    pub region: String,
    pub concurrency: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LocationType {
    S3,
    Local,
}

/// Parse command line arguments
pub fn parse_args() -> Args {
    Args::parse()
}

/// Parse an S3 URL in the format s3://bucket/prefix
fn parse_s3_url(url: &str) -> Result<(String, String)> {
    if !url.starts_with("s3://") {
        return Err(anyhow!(
            "Invalid S3 URL format. Expected s3://bucket/prefix"
        ));
    }

    let path = &url[5..]; // Skip the "s3://" prefix
    let parts: Vec<&str> = path.splitn(2, '/').collect();

    let bucket = parts[0].to_string();
    if bucket.is_empty() {
        return Err(anyhow!("Bucket name cannot be empty"));
    }

    let prefix = parts.get(1).map_or("".to_string(), |s| s.to_string());

    Ok((bucket, prefix))
}

/// Process command line arguments into a SyncConfig
pub fn process_sync_args(args: &Args) -> Result<SyncConfig> {
    match &args.command {
        Command::Sync {
            source,
            destination,
        } => {
            let (source_type, s3_bucket, s3_prefix, local_path) = if source.starts_with("s3://") {
                let (bucket, prefix) = parse_s3_url(source)?;
                (LocationType::S3, Some(bucket), Some(prefix), None)
            } else {
                let path = PathBuf::from(source);
                (LocationType::Local, None, None, Some(path))
            };

            let (dest_type, dest_s3_bucket, dest_s3_prefix, dest_local_path) =
                if destination.starts_with("s3://") {
                    let (bucket, prefix) = parse_s3_url(destination)?;
                    (LocationType::S3, Some(bucket), Some(prefix), None)
                } else {
                    let path = PathBuf::from(destination);
                    (LocationType::Local, None, None, Some(path))
                };

            // Validate source and destination compatibility
            if source_type == LocationType::S3 && dest_type == LocationType::S3 {
                return Err(anyhow!(
                    "S3 to S3 sync is not supported in this implementation"
                ));
            }

            Ok(SyncConfig {
                source_type,
                dest_type,
                s3_bucket,
                s3_prefix,
                local_path,
                dest_s3_bucket,
                dest_s3_prefix,
                dest_local_path,
                region: args.region.clone(),
                concurrency: args.concurrency,
            })
        }
    }
}
