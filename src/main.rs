use std::process::ExitCode;

use stalewise::app::{self, EXIT_CONFIG, EXIT_RUNTIME};
use stalewise::config::Startup;

fn main() -> ExitCode {
    let config = match stalewise::config::load() {
        Ok(Startup::Run(config)) => *config,
        Ok(Startup::PrintCrd) => return print_crd(),
        Err(err) => {
            eprintln!("stalewise: invalid configuration: {err}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };
    let filter = match tracing_subscriber::EnvFilter::try_new(&config.log_level) {
        Ok(filter) => filter,
        Err(err) => {
            eprintln!("stalewise: invalid --log-level: {err}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("stalewise: cannot start runtime: {err}");
            return ExitCode::from(EXIT_RUNTIME);
        }
    };
    runtime.block_on(app::run(config))
}

fn print_crd() -> ExitCode {
    match stalewise::policy::crd_yaml() {
        Ok(yaml) => {
            print!("{yaml}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("stalewise: cannot serialise the CRD: {err}");
            ExitCode::from(EXIT_RUNTIME)
        }
    }
}
