use crate::app::AppState;
use crate::commands::{ConfigCommandHandler, ConfigCommandRegistry};
use crate::pbx::command_helpers::output;
use anyhow::{Result, anyhow};
use std::collections::BTreeMap;
use std::sync::Arc;
use voipswitch_core::command_service::{CommandResult, ConfigCommand};

pub(crate) trait VcConfigTableHandler: Send + Sync {
    fn table(&self) -> &str;
    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<serde_json::Value>;
}

#[derive(Default)]
pub(crate) struct VcConfigTableRegistry {
    handlers: BTreeMap<String, Arc<dyn VcConfigTableHandler>>,
}

impl VcConfigTableRegistry {
    pub(crate) fn register(&mut self, handler: Arc<dyn VcConfigTableHandler>) {
        self.handlers.insert(handler.table().to_string(), handler);
    }
}

pub(crate) fn register_resource(
    registry: &ConfigCommandRegistry,
    table_registry: VcConfigTableRegistry,
) {
    registry.register(Arc::new(VcConfigResource {
        table_registry: Arc::new(table_registry),
    }));
}

struct VcConfigResource {
    table_registry: Arc<VcConfigTableRegistry>,
}

impl ConfigCommandHandler for VcConfigResource {
    fn name(&self) -> &str {
        "vc_config"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<CommandResult> {
        let table = command
            .fields
            .get("table")
            .cloned()
            .or_else(|| command.key.clone())
            .ok_or_else(|| anyhow!("table is required"))?;
        let handler = self
            .table_registry
            .handlers
            .get(&table)
            .ok_or_else(|| anyhow!("unsupported vc config table: {table}"))?;
        Ok(output(command, handler.handle(state, command)?))
    }
}
