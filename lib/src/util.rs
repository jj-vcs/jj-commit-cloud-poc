use std::fs;
use std::path::Path;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommitCloudConfig {
    pub server_url: String,
    pub repo_id: String,
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
