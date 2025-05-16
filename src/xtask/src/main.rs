use std::collections::HashMap;
use std::env;
use std::process::{exit, Command};

fn main() {
    let mut args = env::args().skip(1); // skip the binary name

    // println!("{:?}", args.next());
    let Some(command) = args.next() else {
        print_usage_and_exit();
    };

    // Split into env vars (KEY=VALUE) and passthrough args
    let mut cli_env_vars = HashMap::new();
    let mut passthrough_args = Vec::new();

    for arg in args {
        if let Some((key, value)) = arg.split_once('=') {
            cli_env_vars.insert(key.to_string(), value.to_string());
        } else {
            passthrough_args.push(arg);
        }
    }

    let mut envs = HashMap::new();

    match command.as_str() {
        "build" => {
            debug_build(envs, cli_env_vars, &passthrough_args);
        }
        "clippy" => {
            clippy(envs, cli_env_vars, &passthrough_args);
        }
        "run" => {
            run_build("debug_build", &["run"], envs, cli_env_vars, &passthrough_args);
        }
        "build:release" => {
            run_build("release_build", &["build", "--release"], envs, cli_env_vars, &passthrough_args);
        }
        "run:release" => {
            run_build("release_build", &["run", "--release"], envs, cli_env_vars, &passthrough_args);
        }
        "build:test" | "test" => {
            test(envs, cli_env_vars, &passthrough_args);
        }
        "build:loom" => {
            envs.insert("RUSTFLAGS".into(), "--cfg loom".into());
            run_build("loom_build", &["test", "--release"], envs, cli_env_vars, &passthrough_args);
        }
        "check" => {
            debug_build(envs.clone(), cli_env_vars.clone(), &passthrough_args);
            clippy(envs.clone(), cli_env_vars.clone(), &passthrough_args);
            test(envs, cli_env_vars, &passthrough_args);
        }
        _ => print_usage_and_exit(),
    }
}

fn clippy(envs: HashMap<String, String>, cli_env_vars: HashMap<String, String>, passthrough_args: &[String]) {
    run_build("clippy", &["clippy"], envs, cli_env_vars, passthrough_args);
}

fn test(envs: HashMap<String, String>, cli_env_vars: HashMap<String, String>, passthrough_args: &[String]) {
    run_build("test_build", &["test"], envs, cli_env_vars, passthrough_args);
}

fn debug_build(envs: HashMap<String, String>, cli_env_vars: HashMap<String, String>, passthrough_args: &[String]) {
    run_build("debug_build", &["build"], envs, cli_env_vars, passthrough_args);
}

fn run_build(
    target_dir: &str,
    cargo_args: &[&str],
    mut default_envs: HashMap<String, String>,
    cli_envs: HashMap<String, String>,
    extra_args: &[String],
) {
    // Set target dir
    default_envs.insert("CARGO_TARGET_DIR".into(), format!("target/{}", target_dir));

    // CLI overrides
    for (k, v) in cli_envs {
        default_envs.insert(k, v);
    }

    let mut cmd = Command::new("cargo");
    cmd.args(cargo_args);
    cmd.args(extra_args);

    for (key, value) in &default_envs {
        cmd.env(key, value);
    }

    println!("> Running: cargo {} {}", cargo_args.join(" "), extra_args.join(" "));
    println!("> With envs: {:?}", default_envs);

    let status = cmd.status().expect("Failed to run cargo");
    if !status.success() {
        exit(status.code().unwrap_or(1));
    }
}

fn print_usage_and_exit() -> ! {
    eprintln!(
        "Usage: xtask {{
    build          build in debug mode
    run            runs executable
    build:release  build in release mode
    run:release    runs executable in release mode
    build:test     build and runs tests
    build:loom     builds and tuns loom tests only
    clippy         runs clippy
    check          runs fundamental checks, good to run before push 
    
    [ENV_VAR=value ...] [-- cargo args...]"
    );
    exit(1);
}
