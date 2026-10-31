//! Checks what a client sees through the real REST API.
//!
//! Spawns a lark-server and lark-edge, then runs two suites over plain HTTP:
//!
//! - `spec`: fixed checks of the behavior in Firebase's public REST docs, each
//!   citing its page.
//! - `script`: random scripts of requests compared with a JSON model, with
//!   evictions, restarts and compaction between them.
//!
//! Responses are compared strictly: `{"a": null}` isn't `{}` and `1.0` isn't `1`,
//! as a strict client would see them. Run a debug server, which keeps the checks
//! that only exist in debug builds. See `run.sh`.

mod model;
mod script;
mod spec;
mod stack;

use clap::Parser;
use proptest::test_runner::{Config, RngAlgorithm, TestCaseError, TestError, TestRng, TestRunner};
use stack::{Binaries, Stack};
use std::cell::RefCell;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    server_bin: PathBuf,
    #[arg(long)]
    edge_bin: PathBuf,
    #[arg(long)]
    compact_bin: PathBuf,
    /// Scratch directory for data and logs. Must not hold a previous run.
    #[arg(long)]
    work_dir: PathBuf,
    /// Random scripts to run. 0 skips the script suite.
    #[arg(long, default_value = "100")]
    cases: u32,
    /// Maximum steps per script.
    #[arg(long, default_value = "40")]
    steps: usize,
    /// RNG seed. A failing run replays with the same seed.
    #[arg(long)]
    seed: Option<u64>,
    /// Include restarts and compaction, which take seconds each.
    #[arg(long)]
    slow: bool,
    /// Skip the fixed checks.
    #[arg(long)]
    skip_spec: bool,
    #[arg(long, default_value = "27290")]
    proxy_port: u16,
    #[arg(long, default_value = "18090")]
    edge_port: u16,
}

fn run_scripts(stack: Stack, args: &Args, seed: u64) -> Result<(), String> {
    let config = Config {
        cases: args.cases,
        failure_persistence: None,
        ..Config::default()
    };
    let mut seed_bytes = [0u8; 32];
    seed_bytes[..8].copy_from_slice(&seed.to_le_bytes());
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed_bytes);
    let mut runner = TestRunner::new_with_rng(config, rng);

    let stack = RefCell::new(stack);
    let case = RefCell::new(0u32);
    let result = runner.run(
        &script::script(args.steps, args.slow),
        |(initial, steps)| {
            let mut stack = stack.borrow_mut();
            let mut case = case.borrow_mut();
            *case += 1;
            let root = format!("c{}", *case);
            if (*case).is_multiple_of(25) {
                println!("  {} scripts run", *case);
            }
            let outcome = (|| {
                let mut run = script::Run::new(&mut stack, root);
                run.seed(&initial)?;
                run.apply(&script::Step::Evict)?;
                for step in &steps {
                    run.apply(step).map_err(|e| format!("{step:?}\n{e}"))?;
                }
                run.check_all()
            })();
            if let Some(exit) = stack.server_exit() {
                let log = stack.server_log_tail();
                stack.recover().map_err(TestCaseError::fail)?;
                return Err(TestCaseError::fail(format!(
                    "lark-server died ({exit})\n{log}"
                )));
            }
            outcome.map_err(TestCaseError::fail)
        },
    );
    match result {
        Ok(()) => Ok(()),
        Err(TestError::Abort(reason)) => Err(format!("aborted: {reason}")),
        Err(TestError::Fail(reason, (initial, steps))) => {
            let steps: Vec<String> = steps.iter().map(|s| format!("  {s:?}")).collect();
            Err(format!(
                "{reason}\n\nMinimal script (seed {seed}):\n  seed value: {initial}\n{}",
                steps.join("\n")
            ))
        }
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    let seed = args.seed.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    });
    let binaries = Binaries {
        server: args.server_bin.clone(),
        edge: args.edge_bin.clone(),
        compact: args.compact_bin.clone(),
    };
    let stack = match Stack::start(
        binaries,
        args.work_dir.clone(),
        args.proxy_port,
        args.edge_port,
    ) {
        Ok(stack) => stack,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let mut failed = false;
    if !args.skip_spec {
        failed |= !spec::run(&stack);
    }
    if args.cases > 0 {
        println!("Running {} random scripts, seed {seed}", args.cases);
        match run_scripts(stack, &args, seed) {
            Ok(()) => println!("Random scripts passed"),
            Err(e) => {
                println!("Random scripts FAILED\n{e}");
                failed = true;
            }
        }
    }
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
