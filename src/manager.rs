use std::collections::BTreeMap;

use crate::logfile::Logfile;

/// Manager controls the log file rotation and modification.
///
/// A manager is single threaded to ensure maximum throughput for disk operations.
pub struct Manager {
    options: ManagerOptions,

    log_files: BTreeMap<String, Logfile>,
}

pub struct ManagerOptions {
    log_dir: String,
    max_log_size: usize,
}

impl Manager {
    pub fn with_options(options: ManagerOptions) -> Self {
        Self {
            options,
            log_files: BTreeMap::new(),
        }
    }

    pub fn start(&mut self) -> Result<(), std::io::Error> {
        // Load in all log files in the log directory
        let log_files = std::fs::read_dir(&self.options.log_dir)?;

        for log_file in log_files {
            let log_file = log_file?;
            let path = log_file.path();
            // TODO read the log file metadata
            let logfile = Logfile::new(path.to_string_lossy().to_string());
            self.log_files
                .insert(path.to_string_lossy().to_string(), logfile);
        }

        Ok(())
    }
}
