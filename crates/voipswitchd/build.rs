use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=bpf/rtp_fastpath.bpf.c");
    let out =
        PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is set")).join("rtp_fastpath.bpf.o");
    let status = Command::new("clang")
        .args([
            "-target",
            "bpf",
            "-D__TARGET_ARCH_x86",
            "-O2",
            "-Wall",
            "-Werror",
            "-c",
            "bpf/rtp_fastpath.bpf.c",
            "-o",
        ])
        .arg(&out)
        .status()
        .expect("run clang for RTP fast path");
    assert!(status.success(), "compile RTP fast path eBPF object");
}
