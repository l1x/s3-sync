use anyhow::{anyhow, Context, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_config::Region;
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::ByteStream;
use futures::stream::{self, StreamExt};
use md5::{Digest, Md5};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, create_dir_all, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tokio::task;
use tracing::{error, info, warn};

use crate::cli::{LocationType, SyncConfig};
use crate::progress::ProgressTracker;

/// Main function to run the sync operation
pub async fn run_sync(config: SyncConfig) -> Result<()> {
    // Set up AWS client
    let client = create_s3_client(&config.region).await;

    match (config.source_type, config.dest_type) {
        (LocationType::Local, LocationType::S3) => {
            // Local to S3 sync
            if let (Some(bucket), Some(prefix), Some(local_path)) = (
                config.dest_s3_bucket,
                config.dest_s3_prefix,
                config.local_path.as_ref(),
            ) {
                sync_local_to_s3(&client, local_path, &bucket, &prefix, config.concurrency).await?;
            } else {
                return Err(anyhow!("Missing required parameters for local to S3 sync"));
            }
        }
        (LocationType::S3, LocationType::Local) => {
            // S3 to local sync
            if let (Some(bucket), Some(prefix), Some(local_path)) = (
                config.s3_bucket,
                config.s3_prefix.as_ref(),
                config.dest_local_path.as_ref(),
            ) {
                sync_s3_to_local(&client, &bucket, prefix, local_path, config.concurrency).await?;
            } else {
                return Err(anyhow!("Missing required parameters for S3 to local sync"));
            }
        }
        _ => {
            return Err(anyhow!("Unsupported sync direction"));
        }
    }

    Ok(())
}

/// Sync files from local to S3
async fn sync_local_to_s3(
    client: &Client,
    local_path: &Path,
    bucket: &str,
    prefix: &str,
    concurrency: usize,
) -> Result<()> {
    // Ensure the local path exists
    if !local_path.exists() {
        return Err(anyhow!(
            "Local path does not exist: {}",
            local_path.display()
        ));
    }

    // List all files in the local directory recursively
    let local_files = list_local_files(local_path).await?;
    info!("Found {} local files to process", local_files.len());

    // List all objects in the S3 bucket with the given prefix
    let s3_objects = list_s3_objects(client, bucket, prefix).await?;
    info!("Found {} existing S3 objects", s3_objects.len());

    // Calculate which files need to be uploaded
    let files_to_upload = determine_uploads_to_s3(local_path, &local_files, &s3_objects).await?;
    info!("{} files need to be uploaded", files_to_upload.len());

    if files_to_upload.is_empty() {
        info!("No files need to be uploaded");
        return Ok(());
    }

    // Upload files concurrently
    upload_files(
        client,
        bucket,
        prefix,
        local_path,
        &files_to_upload,
        concurrency,
    )
    .await?;

    info!("Local to S3 sync completed successfully");
    Ok(())
}

/// Sync files from S3 to local
async fn sync_s3_to_local(
    client: &Client,
    bucket: &str,
    prefix: &str,
    local_path: &Path,
    concurrency: usize,
) -> Result<()> {
    // Ensure the local path exists or create it
    if !local_path.exists() {
        create_dir_all(local_path).await?;
    }

    // List all files in the local directory recursively
    let local_files = list_local_files(local_path).await?;
    info!("Found {} local files", local_files.len());

    // List all objects in the S3 bucket with the given prefix
    let s3_objects = list_s3_objects(client, bucket, prefix).await?;
    info!("Found {} S3 objects", s3_objects.len());

    // Calculate which files need to be downloaded
    let files_to_download =
        determine_downloads_to_local(local_path, &local_files, &s3_objects).await?;
    info!("{} files need to be downloaded", files_to_download.len());

    if files_to_download.is_empty() {
        info!("No files need to be downloaded");
        return Ok(());
    }

    // Download files concurrently
    download_files(
        client,
        bucket,
        prefix,
        local_path,
        &files_to_download,
        concurrency,
    )
    .await?;

    info!("S3 to local sync completed successfully");
    Ok(())
}

/// Create an S3 client with the specified region
async fn create_s3_client(region_str: &str) -> Client {
    let region_provider = RegionProviderChain::first_try(Region::new(region_str.to_string()))
        .or_default_provider()
        .or_else(Region::new("us-east-1"));

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;

    Client::new(&config)
}

/// List all files in a directory recursively
async fn list_local_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut result = Vec::new();
    let mut dirs = vec![dir.to_path_buf()];

    while let Some(dir) = dirs.pop() {
        match fs::read_dir(&dir).await {
            Ok(mut entries) => {
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();

                    if path.is_dir() {
                        dirs.push(path);
                    } else {
                        result.push(path);
                    }
                }
            }
            Err(e) => {
                error!("Failed to read directory {}: {}", dir.display(), e);
                // Continue with other directories
            }
        }
    }

    Ok(result)
}

/// List all objects in an S3 bucket with a given prefix
async fn list_s3_objects(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<(String, String)>> {
    let mut result = Vec::new();
    let mut continuation_token = None;

    loop {
        let mut req = client.list_objects_v2().bucket(bucket);

        if !prefix.is_empty() {
            req = req.prefix(prefix);
        }

        if let Some(token) = continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;

        if let Some(contents) = resp.contents {
            for object in contents {
                if let (Some(key), Some(etag)) = (object.key.as_ref(), object.e_tag.as_ref()) {
                    // Remove quotes from etag
                    let clean_etag = etag.trim_matches('"').to_string();
                    result.push((key.to_string(), clean_etag));
                }
            }
        }

        if resp.is_truncated.unwrap_or(false) {
            continuation_token = resp.next_continuation_token.map(|s| s.to_string());
        } else {
            break;
        }
    }

    Ok(result)
}

/// Calculate the MD5 hash of a file to match S3 ETag format
async fn calculate_file_hash(path: &Path) -> Result<String> {
    let mut file = File::open(path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;

    let result = Md5::digest(&buffer);
    Ok(format!("{:x}", result))
}

/// Determine which files need to be uploaded to S3
async fn determine_uploads_to_s3(
    base_dir: &Path,
    local_files: &[PathBuf],
    s3_objects: &[(String, String)],
) -> Result<Vec<PathBuf>> {
    let mut to_upload = Vec::new();

    for local_path in local_files {
        let rel_path = local_path
            .strip_prefix(base_dir)
            .context("Failed to strip prefix from local path")?;

        let s3_key = rel_path.to_string_lossy().replace("\\", "/");

        // Check if the file exists in S3
        if let Some((_, etag)) = s3_objects.iter().find(|(key, _)| key.ends_with(&s3_key)) {
            // If it exists, check if the content is different
            let local_hash = calculate_file_hash(local_path).await?;

            // Note: This is a simplification. In reality, S3 ETag might not be a simple hash
            // for large or multipart uploaded files.
            if &local_hash != etag {
                to_upload.push(local_path.clone());
            }
        } else {
            // File doesn't exist in S3, so it needs to be uploaded
            to_upload.push(local_path.clone());
        }
    }

    Ok(to_upload)
}

/// Determine which files need to be downloaded from S3
async fn determine_downloads_to_local(
    base_dir: &Path,
    local_files: &[PathBuf],
    s3_objects: &[(String, String)],
) -> Result<Vec<String>> {
    let mut to_download = Vec::new();
    let local_file_map: std::collections::HashMap<String, PathBuf> = local_files
        .iter()
        .filter_map(|path| {
            path.strip_prefix(base_dir)
                .ok()
                .map(|rel_path| (rel_path.to_string_lossy().replace("\\", "/"), path.clone()))
        })
        .collect();

    for (s3_key, etag) in s3_objects {
        // let local_rel_path = s3_key.replace("/", &std::path::MAIN_SEPARATOR.to_string());
        // let local_full_path = base_dir.join(&local_rel_path);

        if let Some(local_path) = local_file_map.get(s3_key) {
            // File exists locally, check if it's different
            match calculate_file_hash(local_path).await {
                Ok(local_hash) => {
                    if &local_hash != etag {
                        to_download.push(s3_key.clone());
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to calculate hash for {}: {}",
                        local_path.display(),
                        e
                    );
                    to_download.push(s3_key.clone());
                }
            }
        } else {
            // File doesn't exist locally
            to_download.push(s3_key.clone());
        }
    }

    Ok(to_download)
}

/// Upload files concurrently with a limit on concurrency
async fn upload_files(
    client: &Client,
    bucket: &str,
    prefix: &str,
    base_dir: &Path,
    files: &[PathBuf],
    concurrency: usize,
) -> Result<()> {
    let client = Arc::new(client.clone());
    let semaphore = Arc::new(Semaphore::new(concurrency));

    // Calculate total size for progress reporting
    let mut total_size: u64 = 0;
    for path in files {
        match fs::metadata(path).await {
            Ok(metadata) => {
                total_size += metadata.len();
            }
            Err(e) => {
                warn!("Failed to get metadata for {}: {}", path.display(), e);
            }
        }
    }

    // Initialize progress tracker
    let progress = ProgressTracker::new(files.len());
    {
        let mut tracker = progress.lock().await;
        tracker.set_total_bytes(total_size);
        tracker.start();
    }

    // Start progress reporter in background
    let progress_clone = progress.clone();
    let progress_handle = task::spawn(async move {
        crate::progress::run_progress_reporter(progress_clone).await;
    });

    let upload_tasks = stream::iter(files)
        .map(|path| {
            let client = client.clone();
            let bucket = bucket.to_string();
            let prefix = prefix.to_string();
            let base_dir = base_dir.to_path_buf();
            let path = path.clone();
            let permit = semaphore.clone();
            let progress = progress.clone();

            async move {
                let _permit = permit.acquire().await.unwrap();

                let rel_path = path
                    .strip_prefix(&base_dir)
                    .context("Failed to strip prefix from path")?;

                let s3_key = if prefix.is_empty() {
                    rel_path.to_string_lossy().replace("\\", "/")
                } else {
                    format!(
                        "{}/{}",
                        prefix.trim_end_matches('/'),
                        rel_path.to_string_lossy().replace("\\", "/")
                    )
                };

                info!(
                    "Uploading: {} -> s3://{}/{}",
                    path.display(),
                    bucket,
                    s3_key
                );

                // Get file size for progress tracking
                let file_size = fs::metadata(&path).await.map(|m| m.len()).unwrap_or(0);

                // Read the file
                let body = ByteStream::from_path(&path).await?;

                // Determine content type (basic implementation)
                let content_type = determine_content_type(&path);

                // Upload to S3
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&s3_key)
                    .body(body)
                    .content_type(content_type)
                    .send()
                    .await?;

                // Update progress
                {
                    let mut tracker = progress.lock().await;
                    tracker.update(file_size);
                }

                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>();

    // Wait for all uploads to complete
    let results = upload_tasks.await;

    // Finish progress tracking
    {
        let mut tracker = progress.lock().await;
        tracker.finish();
    }

    // Wait for progress reporter to finish
    let _ = progress_handle.await;

    // Check for errors
    let mut error_count = 0;
    for result in results {
        if let Err(e) = result {
            error_count += 1;
            error!("Error during upload: {}", e);
        }
    }

    if error_count > 0 {
        warn!("{} files failed to upload", error_count);
    }

    Ok(())
}

/// Download files concurrently with a limit on concurrency
async fn download_files(
    client: &Client,
    bucket: &str,
    prefix: &str,
    base_dir: &Path,
    s3_keys: &[String],
    concurrency: usize,
) -> Result<()> {
    let client = Arc::new(client.clone());
    let semaphore = Arc::new(Semaphore::new(concurrency));

    // Initialize progress tracker
    let progress = ProgressTracker::new(s3_keys.len());

    // Get total size for all objects (this requires an extra API call per object)
    let total_size = {
        let mut total_size: u64 = 0;

        for s3_key in s3_keys {
            match client.head_object().bucket(bucket).key(s3_key).send().await {
                Ok(resp) => {
                    if let Some(size) = resp.content_length {
                        total_size += size as u64;
                    }
                }
                Err(e) => {
                    warn!("Failed to get size for s3://{}/{}: {}", bucket, s3_key, e);
                }
            }
        }

        total_size
    };

    // Start progress tracking
    {
        let mut tracker = progress.lock().await;
        tracker.set_total_bytes(total_size);
        tracker.start();
    }
    {
        let mut tracker = progress.lock().await;
        tracker.set_total_bytes(total_size);
        tracker.start();
    }

    // Start progress reporter in background
    let progress_clone = progress.clone();
    let progress_handle = task::spawn(async move {
        crate::progress::run_progress_reporter(progress_clone).await;
    });

    let download_tasks = stream::iter(s3_keys)
        .map(|s3_key| {
            let client = client.clone();
            let bucket = bucket.to_string();
            let s3_key = s3_key.clone();
            let base_dir = base_dir.to_path_buf();
            let permit = semaphore.clone();
            let prefix = prefix.to_string();
            let progress = progress.clone();

            async move {
                let _permit = permit.acquire().await.unwrap();

                // Calculate the local file path
                let obj_key = if !prefix.is_empty() && s3_key.starts_with(&prefix) {
                    // Remove the prefix from the key to get the relative path
                    &s3_key[prefix.len()..]
                } else {
                    &s3_key
                };

                let obj_key = obj_key.trim_start_matches('/');
                let local_rel_path = obj_key.replace("/", &std::path::MAIN_SEPARATOR.to_string());
                let local_path = base_dir.join(&local_rel_path);

                info!(
                    "Downloading: s3://{}/{} -> {}",
                    bucket,
                    s3_key,
                    local_path.display()
                );

                // Create parent directories if they don't exist
                if let Some(parent) = local_path.parent() {
                    if !parent.exists() {
                        create_dir_all(parent).await?;
                    }
                }

                // Get the object from S3
                let resp = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&s3_key)
                    .send()
                    .await?;

                let content_length = resp.content_length.unwrap_or(0) as u64;

                // Stream the response to a file
                let mut file = File::create(&local_path).await?;
                let body = resp.body;

                // Get the full content and write it to the file
                let data = body.collect().await?;
                file.write_all(&data.into_bytes()).await?;

                // Update progress
                {
                    let mut tracker = progress.lock().await;
                    tracker.update(content_length);
                }

                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>();

    // Wait for all downloads to complete
    let results = download_tasks.await;

    // Finish progress tracking
    {
        let mut tracker = progress.lock().await;
        tracker.finish();
    }

    // Wait for progress reporter to finish
    let _ = progress_handle.await;

    // Check for errors
    let mut error_count = 0;
    for result in results {
        if let Err(e) = result {
            error_count += 1;
            error!("Error during download: {}", e);
        }
    }

    if error_count > 0 {
        warn!("{} files failed to download", error_count);
    }

    Ok(())
}

/// Determine the content type based on file extension
fn determine_content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("png") => "image/png",
        Some("txt") => "text/plain",
        Some("html") => "text/html",
        Some("css") => "text/css",
        Some("js") => "application/javascript",
        Some("json") => "application/json",
        Some("pdf") => "application/pdf",
        _ => "application/octet-stream",
    }
}
