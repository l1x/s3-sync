use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::info;

#[derive(Clone, Debug)]
pub struct ProgressTracker {
    start_time: Instant,
    last_update: Instant,
    total_bytes: u64,
    processed_bytes: u64,
    total_files: usize,
    processed_files: usize,
    remaining_files: usize,
    in_progress: bool,
}

impl ProgressTracker {
    pub fn new(total_files: usize) -> Arc<Mutex<Self>> {
        let now = Instant::now();
        Arc::new(Mutex::new(Self {
            start_time: now,
            last_update: now,
            total_bytes: 0,
            processed_bytes: 0,
            total_files,
            processed_files: 0,
            remaining_files: total_files,
            in_progress: false,
        }))
    }

    pub fn start(&mut self) {
        self.in_progress = true;
        self.start_time = Instant::now();
        self.last_update = self.start_time;
    }

    pub fn update(&mut self, file_size: u64) {
        self.processed_bytes += file_size;
        self.processed_files += 1;
        self.remaining_files = self.total_files - self.processed_files;
    }

    pub fn set_total_bytes(&mut self, total: u64) {
        self.total_bytes = total;
    }

    // pub fn add_total_bytes(&mut self, bytes: u64) {
    //     self.total_bytes += bytes;
    // }

    fn format_bytes(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;
        const TB: u64 = GB * 1024;

        if bytes >= TB {
            format!("{:.1} TiB", bytes as f64 / TB as f64)
        } else if bytes >= GB {
            format!("{:.1} GiB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.1} MiB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.1} KiB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }

    fn format_speed(&self) -> String {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed <= 0.0 {
            return "0 B/s".to_string();
        }

        let bytes_per_second = self.processed_bytes as f64 / elapsed;
        Self::format_bytes(bytes_per_second as u64) + "/s"
    }

    fn get_progress_message(&self) -> String {
        let processed_bytes_str = Self::format_bytes(self.processed_bytes);
        let total_bytes_str = if self.total_bytes > 0 {
            format!("~{}", Self::format_bytes(self.total_bytes))
        } else {
            "calculating...".to_string()
        };

        let speed = self.format_speed();

        if self.remaining_files > 0 {
            format!(
                "Completed {}/{} ({}) with ~{} file(s) remaining ({})",
                processed_bytes_str,
                total_bytes_str,
                speed,
                self.remaining_files,
                if self.total_bytes == 0 {
                    "calculating..."
                } else {
                    ""
                }
            )
        } else {
            format!("Completed {} at {}", processed_bytes_str, speed)
        }
    }

    pub fn finish(&mut self) {
        self.in_progress = false;
        let total_time = self.start_time.elapsed().as_secs_f64();
        let speed = if total_time > 0.0 {
            self.processed_bytes as f64 / total_time
        } else {
            0.0
        };

        info!(
            "Transfer completed: {} transferred in {:.1}s ({}/s)",
            Self::format_bytes(self.processed_bytes),
            total_time,
            Self::format_bytes(speed as u64)
        );
    }
}

pub async fn run_progress_reporter(progress: Arc<Mutex<ProgressTracker>>) {
    let update_interval = Duration::from_millis(500);

    loop {
        sleep(update_interval).await;

        let is_running = {
            let tracker = progress.lock().await;
            if tracker.in_progress && (tracker.last_update.elapsed() >= update_interval) {
                let message = tracker.get_progress_message();
                // Use print! instead of info! to overwrite the line
                print!("\r{}", message);
                // Flush stdout to ensure the message is displayed
                use std::io::Write;
                let _ = std::io::stdout().flush();
                true
            } else {
                tracker.in_progress
            }
        };

        if !is_running {
            break;
        }
    }

    // Print a newline at the end to ensure next output starts on a new line
    println!();
}
