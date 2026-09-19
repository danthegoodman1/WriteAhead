use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, Weak};
use std::time::Duration;
use writeahead::{FileIo, SimpleFile};

static CONTROLS: Mutex<BTreeMap<PathBuf, Weak<Control>>> = Mutex::new(BTreeMap::new());

#[derive(Debug)]
pub struct Control {
    dir: PathBuf,
    pub reads: AtomicUsize,
    pub stats: AtomicUsize,
    pub syncs: AtomicUsize,
    pub fail_read: AtomicU64,
    pub fail_open: AtomicBool,
    pub fail_sync: AtomicBool,
    gate: Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>,
    handles: Mutex<BTreeMap<PathBuf, usize>>,
}

impl Control {
    pub fn new(dir: &Path) -> Arc<Self> {
        let control = Arc::new(Self {
            dir: dir.to_path_buf(),
            reads: AtomicUsize::new(0),
            stats: AtomicUsize::new(0),
            syncs: AtomicUsize::new(0),
            fail_read: AtomicU64::new(u64::MAX),
            fail_open: AtomicBool::new(false),
            fail_sync: AtomicBool::new(false),
            gate: Mutex::new(None),
            handles: Mutex::new(BTreeMap::new()),
        });
        CONTROLS
            .lock()
            .unwrap()
            .insert(dir.to_path_buf(), Arc::downgrade(&control));
        control
    }

    pub fn pause_sync(&self) -> PausedSync {
        let (entered_tx, entered) = mpsc::channel();
        let (resume, resume_rx) = mpsc::channel();
        *self.gate.lock().unwrap() = Some((entered_tx, resume_rx));
        PausedSync { entered, resume }
    }

    pub fn handles(&self, path: &Path) -> usize {
        self.handles.lock().unwrap().get(path).copied().unwrap_or(0)
    }
}

impl Drop for Control {
    fn drop(&mut self) {
        CONTROLS.lock().unwrap().remove(&self.dir);
    }
}

pub struct PausedSync {
    entered: mpsc::Receiver<()>,
    resume: mpsc::Sender<()>,
}

impl PausedSync {
    pub fn wait(&self) {
        self.entered.recv_timeout(Duration::from_secs(10)).unwrap();
    }
}

impl Drop for PausedSync {
    fn drop(&mut self) {
        let _ = self.resume.send(());
    }
}

#[derive(Debug)]
pub struct FaultFile {
    inner: SimpleFile,
    path: PathBuf,
    control: Arc<Control>,
}

impl FaultFile {
    fn open_with(path: &Path, existing: bool) -> anyhow::Result<Self> {
        let control = CONTROLS
            .lock()
            .unwrap()
            .get(path.parent().unwrap())
            .and_then(Weak::upgrade)
            .expect("register a Control before opening test files");
        if existing && control.fail_open.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected open failure");
        }
        let inner = if existing {
            SimpleFile::open_existing(path)?
        } else {
            SimpleFile::open(path)?
        };
        *control
            .handles
            .lock()
            .unwrap()
            .entry(path.to_path_buf())
            .or_default() += 1;
        Ok(Self {
            inner,
            path: path.to_path_buf(),
            control,
        })
    }
}

impl Drop for FaultFile {
    fn drop(&mut self) {
        let mut handles = self.control.handles.lock().unwrap();
        let count = handles.get_mut(&self.path).unwrap();
        *count -= 1;
        if *count == 0 {
            handles.remove(&self.path);
        }
    }
}

impl FileIo for FaultFile {
    fn open(path: &Path) -> anyhow::Result<Self> {
        Self::open_with(path, false)
    }
    fn open_existing(path: &Path) -> anyhow::Result<Self> {
        Self::open_with(path, true)
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.control.reads.fetch_add(1, Ordering::SeqCst);
        let fault = self.control.fail_read.load(Ordering::SeqCst);
        if offset <= fault && fault < offset + buf.len() as u64 {
            return Err(std::io::Error::from_raw_os_error(5).into());
        }
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        self.inner.write_at(offset, data)
    }
    fn sync(&mut self) -> anyhow::Result<()> {
        self.control.syncs.fetch_add(1, Ordering::SeqCst);
        let gate = self.control.gate.lock().unwrap().take();
        if let Some((entered, resume)) = gate {
            let _ = entered.send(());
            let _ = resume.recv_timeout(Duration::from_secs(10));
        }
        if self.control.fail_sync.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected sync failure");
        }
        self.inner.sync()
    }
    fn len(&self) -> anyhow::Result<u64> {
        self.control.stats.fetch_add(1, Ordering::SeqCst);
        self.inner.len()
    }
    fn set_len(&mut self, len: u64) -> anyhow::Result<()> {
        self.inner.set_len(len)
    }
}
