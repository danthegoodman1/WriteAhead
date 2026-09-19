//! Separate binary: a concurrent fork can briefly inherit other tests' directory locks.
use writeahead::write_ahead::WriteAheadError;
use writeahead::{SimpleFile, WriteAhead, WriteAheadOptions};

fn options(dir: &std::path::Path) -> WriteAheadOptions {
    WriteAheadOptions {
        log_dir: dir.into(),
        preallocation_chunk_size: None,
        ..Default::default()
    }
}

#[test]
fn directory_owner_child() {
    let Some(path) = std::env::var_os("WRITEAHEAD_TEST_OWNER_DIR") else {
        return;
    };
    let mut wal = WriteAhead::<SimpleFile>::with_options(options(std::path::Path::new(&path)));
    let locked = std::env::var_os("WRITEAHEAD_TEST_EXPECT_LOCKED").is_some();
    if locked {
        assert!(matches!(
            wal.start().unwrap_err().downcast_ref::<WriteAheadError>(),
            Some(WriteAheadError::DirectoryLocked)
        ));
    } else {
        wal.start().unwrap();
    }
}

#[test]
fn directory_ownership_excludes_other_processes_and_releases_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = WriteAhead::<SimpleFile>::with_options(options(dir.path()));
    wal.start().unwrap();
    for locked in [true, false] {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", "directory_owner_child"])
            .env("WRITEAHEAD_TEST_OWNER_DIR", dir.path());
        if locked {
            child.env("WRITEAHEAD_TEST_EXPECT_LOCKED", "1");
        } else {
            child.env_remove("WRITEAHEAD_TEST_EXPECT_LOCKED");
        }
        let output = child.output().unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if locked {
            wal = WriteAhead::with_options(options(dir.path()));
        }
    }
    drop(wal);
}
