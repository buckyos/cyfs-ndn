fn main() {
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_OS");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_ARCH");
    let cargo_target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let cargo_target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    let arch = match cargo_target_arch.as_str() {
        "x86_64" => "amd64",
        "aarch64" => "aarch64",
        unsupported => panic!("unsupported package target architecture: {unsupported}"),
    };
    let os = match cargo_target_os.as_str() {
        "linux" => "linux",
        "windows" => "windows",
        "macos" => "apple",
        unsupported => panic!("unsupported package target OS: {unsupported}"),
    };
    // TODO: 这里的nightly也要通过某个环境变量指定
    let default_prefix = format!("nightly-{}-{}", os, arch);
    println!("cargo::rustc-env=PACKAGE_DEFAULT_PREFIX={}", default_prefix);
    // Keep the misspelled variable during the compatibility window.
    println!("cargo::rustc-env=PACKAGE_DEFAULT_PERFIX={}", default_prefix);
}
