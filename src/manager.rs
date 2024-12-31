use std::collections::{BTreeMap, BTreeSet};

use crate::logfile::Logfile;

/// Manager controls the log file rotation and modification.
///
/// A manager is single threaded to ensure maximum throughput for disk operations.
pub struct Manager {
    options: ManagerOptions,

    logfiles: BTreeMap<String, Logfile>,
}

pub struct ManagerOptions {
    log_dir: String,
    max_log_size: usize,
}

impl Manager {
    pub fn with_options(options: ManagerOptions) -> Self {
        Self {
            options,
            logfiles: BTreeMap::new(),
        }
    }

    pub fn start(&self) -> Result<(), std::io::Error> {
        // Load in all logfiles in the log directory
        let log_files = std::fs::read_dir(&self.options.log_dir)?;

        Ok(())
    }
}
