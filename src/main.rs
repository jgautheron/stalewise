use std::process::ExitCode;

const EXIT_INVALID_CONFIG: u8 = 2;

fn main() -> ExitCode {
    match stalewise::config::load() {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("stalewise: invalid configuration: {err}");
            ExitCode::from(EXIT_INVALID_CONFIG)
        }
    }
}
