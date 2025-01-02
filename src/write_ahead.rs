use anyhow::Result;
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use anyhow::Context;

use crate::{fileio::FileIO, logfile::Logfile};

/// Manager controls the log file rotation and modification.
///
/// A manager is single threaded to ensure maximum throughput for disk operations.
pub struct WriteAhead<F: FileIO> {
    options: WriteAheadOptions,

    log_files: BTreeMap<String, Logfile<F>>,
}

#[derive(Debug)]
pub struct WriteAheadOptions {
    pub log_dir: PathBuf,
    pub max_log_size: usize,
    pub retention: RetentionOptions,
}

impl Default for WriteAheadOptions {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("./write_ahead"),
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

impl<F: FileIO> WriteAhead<F> {
    pub fn with_options(options: WriteAheadOptions) -> Self {
        Self {
            options,
            log_files: BTreeMap::new(),
        }
    }

    /// Start the write ahead log manager.
    /// If this errors, you must crash.
    pub fn start(&mut self) -> Result<()> {
        let log_files =
            std::fs::read_dir(&self.options.log_dir).context("Failed to read log directory")?;

        // for log_file in log_files {
        //     let log_file = log_file.context("Failed to get dir entry")?;
        //     let path = log_file.path();
        //     let file_id = Logfile::file_id_from_path(&path)?;

        //     let logfile = Logfile::new(file_id, F::open(&path)?);
        //     self.log_files.insert(file_id, logfile);
        // }

        Ok(())
    }
}
