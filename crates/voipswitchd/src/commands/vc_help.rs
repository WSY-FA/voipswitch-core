use crate::app::AppState;
use crate::commands::{ApiCommandHandler, text_result};
use anyhow::Result;
use voipswitch_core::command_service::{ApiCommand, CommandResult};

pub(super) struct VcHelpApiCommand;

impl ApiCommandHandler for VcHelpApiCommand {
    fn name(&self) -> &str {
        "vc"
    }

    fn handle(&self, _state: &AppState, _command: &ApiCommand) -> Result<CommandResult> {
        Ok(text_result(
            "vc config help",
            vec![
                "vc config table commands:".to_string(),
                "".to_string(),
                "  vc config select <table> [--domain <domain>] [--cond <field=value|any>] [--keys <k1,k2,...>] [--page <n>] [--page-size <n>]".to_string(),
                "  vc config count <table> [--domain <domain>] [--cond <field=value|any>]".to_string(),
                "  vc config insert <table> [--domain <domain>] <k1=v1> <k2=v2> ...".to_string(),
                "  vc config update <table> [--domain <domain>] [--cond <field=value|any>] <k1=v1> <k2=v2> ...".to_string(),
                "  vc config delete <table> [--domain <domain>] [--cond <field=value|any>]".to_string(),
                "  vc config batch_insert <table> [--domain <domain>] --file <path>".to_string(),
                "".to_string(),
                "config tables: global_setting, domain, ext, peer_trunk, reg_trunk, reg_account, inbound_route, outbound_route, recording_policy, ai_policy".to_string(),
                "status tables: ext_status, siptrk_status (select only)".to_string(),
                "".to_string(),
                "examples:".to_string(),
                "  vc config select domain".to_string(),
                "  vc config select global_setting".to_string(),
                "  vc config update global_setting --cond key=call_trace_enabled value=false".to_string(),
                "  vc config update global_setting --cond any sip_port=5060 log_level=info call_trace_enabled=true".to_string(),
                "  vc config select ext --domain domain-xxx".to_string(),
                "  vc config select ext --domain domain-xxx --page 1 --page-size 50".to_string(),
                "  vc config count ext --domain domain-xxx".to_string(),
                "  vc config select ext_status --domain domain-xxx".to_string(),
                "  vc config select siptrk_status --domain domain-xxx".to_string(),
                "  vc config insert ext --domain domain-xxx number=1001 auth_user=1001 password=1234 enabled=true".to_string(),
                "  vc config update ext --domain domain-xxx --cond id=1 password=5678".to_string(),
                "  vc config delete ext --domain domain-xxx --cond id=1".to_string(),
                "  vc config insert peer_trunk --domain domain-xxx name=carrier-a server_host=sip.example.com transport=udp".to_string(),
                "  vc config insert reg_trunk --domain domain-xxx name=carrier-b server_host=sip.example.net requested_expires_seconds=300".to_string(),
                "  vc config insert reg_account --domain domain-xxx reg_trunk_id=1 auth_name=user auth_pwd=secret".to_string(),
                "  vc config insert outbound_route --domain domain-xxx name=pstn dst_pattern=^9 priority=100 trunk_refs=peer:1,reg:1/1".to_string(),
                "  vc config insert inbound_route --domain domain-xxx name=did trunk_match=peer:1 dst_pattern=^400 target=auto priority=100".to_string(),
                "  vc config update outbound_route --domain domain-xxx --cond id=1 priority=10 trunk_refs=reg:1/1,peer:1".to_string(),
                "  vc config delete inbound_route --domain domain-xxx --cond id=1".to_string(),
                "  vc config insert recording_policy --domain domain-xxx name=record-1001 target_type=extension target_id=1 direction=both priority=100 enabled=true".to_string(),
                "  vc config insert ai_policy --domain domain-xxx name=ai-1001 target_refs=ext:1 direction=any priority=100 ai_profile_id=profile-1 enabled=true".to_string(),
            ],
        ))
    }
}
