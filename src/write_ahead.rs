use std::collections::BTreeMap;

use crate::logfile::Logfile;

/// Manager controls the log file rotation and modification.
///
/// A manager is single threaded to ensure maximum throughput for disk operations.
pub struct WriteAhead {
    options: WriteAheadOptions,

    log_files: BTreeMap<String, Logfile>,
}

pub struct WriteAheadOptions {
    log_dir: String,
    max_log_size: usize,
}

impl Default for WriteAheadOptions {
    fn default() -> Self {
        Self {
            log_dir: "./write_ahead".to_string(),
            max_log_size: 1024 * 1024 * 1024, // 1GB
        }
    }
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
            self.log_files.insert(file_name.to_string(), logfile);
        }

        Ok(())
    }
}
