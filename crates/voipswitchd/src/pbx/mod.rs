pub(crate) mod ai_policy;
mod command_helpers;
pub mod domain;
pub mod extension;
mod global_setting;
mod migration;
mod module;
pub mod recording;
pub mod route;
mod sip;
pub mod trunk;
mod user;
mod vc_config;

use crate::app::AppState;
use crate::commands::{ApiCommandRegistry, ConfigCommandRegistry};
use anyhow::Result;
use module::{PbxModule, PbxModuleDescriptor, PbxModuleInitContext};
use voipswitch_core::analysis::AnalysisRegistry;
use voipswitch_core::command_service::{ApiCommand, CommandResult};

const PBX_MODULES: &[PbxModuleDescriptor] = &[
    PbxModuleDescriptor {
        init: ai_policy::Module::init,
        handle_show: ai_policy::Module::handle_show,
    },
    PbxModuleDescriptor {
        init: domain::Module::init,
        handle_show: domain::Module::handle_show,
    },
    PbxModuleDescriptor {
        init: extension::Module::init,
        handle_show: extension::Module::handle_show,
    },
    PbxModuleDescriptor {
        init: trunk::Module::init,
        handle_show: trunk::Module::handle_show,
    },
    PbxModuleDescriptor {
        init: route::Module::init,
        handle_show: route::Module::handle_show,
    },
    PbxModuleDescriptor {
        init: recording::Module::init,
        handle_show: recording::Module::handle_show,
    },
];

#[derive(Clone)]
#[allow(dead_code)]
pub struct PbxServices {
    pub analysis: AnalysisRegistry,
}

impl PbxServices {
    pub fn new() -> Self {
        Self {
            analysis: AnalysisRegistry::default(),
        }
    }

    pub fn analyzer_names(&self) -> Vec<String> {
        self.analysis.names()
    }
}

pub fn init_modules(
    state: AppState,
    analysis: &AnalysisRegistry,
    config_commands: &ConfigCommandRegistry,
    api_commands: &ApiCommandRegistry,
) {
    let mut vc_config_tables = vc_config::VcConfigTableRegistry::default();
    let mut ctx = PbxModuleInitContext {
        state,
        analysis,
        vc_config_tables: &mut vc_config_tables,
    };

    for module in PBX_MODULES {
        (module.init)(&mut ctx);
    }
    sip::register_vc_config_tables(&mut vc_config_tables);
    global_setting::register_vc_config_table(&mut vc_config_tables);
    sip::register_config_commands(config_commands);
    user::register_api_commands(api_commands);
    vc_config::register_resource(config_commands, vc_config_tables);
}

pub fn handle_show_command(
    state: &AppState,
    command: &ApiCommand,
) -> Result<Option<CommandResult>> {
    for module in PBX_MODULES {
        if let Some(result) = (module.handle_show)(state, command)? {
            return Ok(Some(result));
        }
    }
    Ok(None)
}
