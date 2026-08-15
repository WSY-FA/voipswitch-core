use crate::app::AppState;
use crate::pbx::vc_config::VcConfigTableRegistry;
use anyhow::Result;
use voipswitch_core::analysis::AnalysisRegistry;
use voipswitch_core::command_service::{ApiCommand, CommandResult};

pub(crate) struct PbxModuleInitContext<'a> {
    pub state: AppState,
    pub analysis: &'a AnalysisRegistry,
    pub vc_config_tables: &'a mut VcConfigTableRegistry,
}

pub(crate) trait PbxModule {
    fn init(ctx: &mut PbxModuleInitContext<'_>);

    fn handle_show(_state: &AppState, _command: &ApiCommand) -> Result<Option<CommandResult>> {
        Ok(None)
    }
}

pub(crate) struct PbxModuleDescriptor {
    pub init: fn(&mut PbxModuleInitContext<'_>),
    pub handle_show: fn(&AppState, &ApiCommand) -> Result<Option<CommandResult>>,
}
