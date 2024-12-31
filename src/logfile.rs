use std::time::Instant;

pub struct Logfile {
    created_at: Instant,
    sealed_at: Option<Instant>,
    path: String,
}

impl Logfile {
    pub fn new(path: String) -> Self {
        Self {
            created_at: Instant::now(),
            sealed_at: None,
            path,
        }
    }
}
