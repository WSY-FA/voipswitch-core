use super::RuntimeConfig;
use std::sync::{Arc, RwLock};
use tokio::sync::watch;

#[derive(Debug)]
pub struct RuntimeConfigStore {
    current: RwLock<Arc<RuntimeConfig>>,
    updates: watch::Sender<Arc<RuntimeConfig>>,
}

impl RuntimeConfigStore {
    pub fn new(initial: RuntimeConfig) -> Self {
        let initial = Arc::new(initial);
        let (updates, _) = watch::channel(initial.clone());
        Self {
            current: RwLock::new(initial),
            updates,
        }
    }

    pub fn snapshot(&self) -> Arc<RuntimeConfig> {
        self.current
            .read()
            .expect("runtime config lock poisoned")
            .clone()
    }

    pub fn replace(&self, mut next: RuntimeConfig) -> Arc<RuntimeConfig> {
        let mut current = self.current.write().expect("runtime config lock poisoned");
        next.version = next.version.max(current.version.saturating_add(1));
        let next = Arc::new(next);
        *current = next.clone();
        self.updates.send_replace(next.clone());
        next
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<RuntimeConfig>> {
        self.updates.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_service::SystemConfig;
    use std::collections::BTreeMap;

    #[test]
    fn replacement_version_is_process_local_and_monotonic() {
        let store = RuntimeConfigStore::new(runtime_config(7));

        assert_eq!(store.replace(runtime_config(3)).version, 8);
        assert_eq!(store.replace(runtime_config(20)).version, 20);
        assert_eq!(store.replace(runtime_config(20)).version, 21);
    }

    #[test]
    fn subscribers_receive_replacements() {
        let store = RuntimeConfigStore::new(runtime_config(1));
        let receiver = store.subscribe();

        store.replace(runtime_config(1));

        assert_eq!(receiver.borrow().version, 2);
    }

    fn runtime_config(version: u64) -> RuntimeConfig {
        RuntimeConfig {
            system: SystemConfig {
                instance_id: "test".to_string(),
                data_dir: "/tmp/test".to_string(),
            },
            globals: BTreeMap::new(),
            domains: BTreeMap::new(),
            version,
        }
    }
}
