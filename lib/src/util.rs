use std::fs;
use std::path::Path;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommitCloudConfig {
    pub server_url: String,
    pub repo_id: String,
}

impl CommitCloudConfig {
    // Resolves the repository configuration file path (`.jj/repo/store/config.toml`).
    // When called from op_store or op_heads_store, store_path points to .jj/repo/op_store or
    // .jj/repo/op_heads, so fallback to the parent directory (.jj/repo/store/config.toml).
    pub fn load_from_store(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config_path = store_path.join("config.toml");
        if config_path.exists() {
            let content = fs::read_to_string(&config_path)?;
            Ok(toml::from_str(&content)?)
        } else if let Some(parent) = store_path.parent() {
            let store_config = parent.join("store").join("config.toml");
            let content = fs::read_to_string(&store_config)?;
            Ok(toml::from_str(&content)?)
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "store path should have contained config.toml",
            )))
        }
    }
}

pub fn run_async<F, Fut, T>(f: F) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
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
