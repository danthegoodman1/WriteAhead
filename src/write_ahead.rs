use std::{collections::BTreeMap, time::Duration};

use crate::logfile::Logfile;

/// Manager controls the log file rotation and modification.
///
/// A manager is single threaded to ensure maximum throughput for disk operations.
pub struct WriteAhead {
    options: WriteAheadOptions,

    log_files: BTreeMap<u64, Logfile>,
}

#[derive(Debug)]
pub struct WriteAheadOptions {
    pub log_dir: String,
    pub max_log_size: usize,
    pub retention: RetentionOptions,
}

impl Default for WriteAheadOptions {
    fn default() -> Self {
        Self {
            log_dir: "./write_ahead".to_string(),
            max_log_size: 1024 * 1024 * 1024, // 1GB
            retention: RetentionOptions::default(),
        }
    }
}

#[derive(Default, Debug)]
pub struct RetentionOptions {
    /// The maximum total size of all log files. Set to `0` to disable.
    pub max_total_size: usize,
    /// The maximum age of the log file, determined by the sealed timestamp. Set to `0` to disable.
    pub ttl: Duration,
}

impl WriteAhead {
    pub fn with_options(options: WriteAheadOptions) -> Self {
        Self {
            options,
            log_files: BTreeMap::new(),
        }
    }

    pub fn start(&mut self) -> Result<(), std::io::Error> {
        let log_files = std::fs::read_dir(&self.options.log_dir)?;

        for log_file in log_files {
            let log_file = log_file?;
            let path = log_file.path();
            let file_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse::<u64>().ok())
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid log file name - must be a number", // TODO make custom error type?
                    )
                })?;

            let logfile = Logfile::new(file_name);
            self.log_files.insert(file_name, logfile);
        }

        Ok(())
    }
}
