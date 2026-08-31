use std::fs;
use std::path::Path;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommitCloudConfig {
    pub server_url: String,
    pub repo_id: String,
    #[serde(default = "default_use_daemon")]
    pub use_daemon: bool,
    #[serde(default)]
    pub daemon_socket: Option<String>,
}

fn default_use_daemon() -> bool {
    true
}

impl CommitCloudConfig {
    // Resolves the repository configuration file path (`.jj/repo/store/config.toml`).
    // Searches within store_path, sibling directories, and parent .jj/repo/store/ paths.
    pub fn load_from_store(
        store_path: &Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config_path = store_path.join("config.toml");
        if config_path.exists() {
            let content = fs::read_to_string(&config_path)?;
            return Ok(toml::from_str(&content)?);
        }

        let mut curr = store_path.to_path_buf();
        for _ in 0..4 {
            let candidate0 = curr.join(".jj/repo/store/config.toml");
            if candidate0.exists() {
                let content = fs::read_to_string(&candidate0)?;
                return Ok(toml::from_str(&content)?);
            }
            let candidate1 = curr.join("store").join("config.toml");
            if candidate1.exists() {
                let content = fs::read_to_string(&candidate1)?;
                return Ok(toml::from_str(&content)?);
            }
            let candidate2 = curr.join("repo").join("store").join("config.toml");
            if candidate2.exists() {
                let content = fs::read_to_string(&candidate2)?;
                return Ok(toml::from_str(&content)?);
            }
            if !curr.pop() {
                break;
            }
        }

        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "store path '{}' should have contained config.toml",
                store_path.display()
            ),
        )))
    }

    pub fn save_to_store(
        &self,
        store_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let content = toml::to_string_pretty(self)?;
        let config_path = store_path.join("config.toml");
        if config_path.exists() {
            fs::write(&config_path, content)?;
            return Ok(());
        }

        let mut curr = store_path.to_path_buf();
        for _ in 0..4 {
            let candidate0 = curr.join(".jj/repo/store/config.toml");
            if candidate0.exists() {
                fs::write(&candidate0, content)?;
                return Ok(());
            }
            let candidate1 = curr.join("store").join("config.toml");
            if candidate1.exists() {
                fs::write(&candidate1, content)?;
                return Ok(());
            }
            let candidate2 = curr.join("repo").join("store").join("config.toml");
            if candidate2.exists() {
                fs::write(&candidate2, content)?;
                return Ok(());
            }
            if !curr.pop() {
                break;
            }
        }

        // Fallback: write directly under .jj/repo/store/config.toml
        let target = store_path.join(".jj/repo/store/config.toml");
        if let Some(parent) = target.parent() {
            let _ = fs::create_dir_all(parent);
        }
        fs::write(&target, content)?;
        Ok(())
    }
}

pub fn run_async<F, Fut, T>(f: F) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>
        + Send
        + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(f())
    })
    .join()
    .map_err(|e| format!("Thread join error: {:?}", e))?
}
