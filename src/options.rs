//! Command-line options for airflow.

/// Operating mode
#[derive(Debug, Clone, PartialEq)]
pub enum Mode {
    /// Default mode
    Default,
    /// Create mode
    Create,
    /// Run mode
    Run,
    /// Schedule mode
    Schedule,
    /// Status mode
    Status,
    /// Config mode
    Config,
}

/// Command-line options
#[derive(Debug, Clone)]
pub struct Options {
    /// Operating mode
    pub mode: Mode,
    /// Config file path
    pub config_path: String,
    /// Verbose output
    pub verbose: bool,
    /// Quiet mode
    pub quiet: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            mode: Mode::Default,
            config_path: String::new(),
            verbose: false,
            quiet: false,
        }
    }
}
