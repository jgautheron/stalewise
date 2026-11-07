use std::process::ExitCode;

use stalewise::app::{self, EXIT_CONFIG, EXIT_RUNTIME};

fn main() -> ExitCode {
    let config = match stalewise::config::load() {
        Ok(config) => config,
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
