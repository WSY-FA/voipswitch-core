pub mod config;
pub mod model;

use crate::pbx::module::{PbxModule, PbxModuleInitContext};

pub(crate) struct Module;

impl PbxModule for Module {
    fn init(ctx: &mut PbxModuleInitContext<'_>) {
        config::register_vc_config_table(ctx.vc_config_tables);
    }
}
