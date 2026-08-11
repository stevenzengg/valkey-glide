/**
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 */
use once_cell::sync::OnceCell;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::{
    path::{Path, PathBuf},
    sync::RwLock,
};
use tracing::{self, event};
use tracing_appender::rolling::{RollingFileAppender, RollingWriter, Rotation};
use tracing_subscriber::{
    Registry,
    filter::Filtered,
    fmt::{
        Layer,
        format::{DefaultFields, Format},
    },
    layer::Layered,
};

use tracing_subscriber::{
    self,
    filter::{self, LevelFilter},
    prelude::*,
    reload::{self, Handle},
};

use std::str::FromStr;

// Layer-Filter pair determines whether a log will be collected
type InnerFiltered = Filtered<Layer<Registry>, LevelFilter, Registry>;
// A Reloadable pair of layer-filter
type InnerLayered = Layered<reload::Layer<InnerFiltered, Registry>, Registry>;
// A reloadable layer of subscriber to a rolling file
type FileReload = Handle<
    Filtered<
        Layer<InnerLayered, DefaultFields, Format, LazyRollingFileAppender>,
        LevelFilter,
        InnerLayered,
    >,
    InnerLayered,
>;

pub struct Reloads {
    console_reload: RwLock<reload::Handle<InnerFiltered, Registry>>,
    file_reload: RwLock<FileReload>,
}

pub struct InitiateOnce {
    init_once: OnceCell<Reloads>,
}

pub static INITIATE_ONCE: InitiateOnce = InitiateOnce {
    init_once: OnceCell::new(),
};

const FILE_DIRECTORY: &str = "glide-logs";
const ENV_GLIDE_LOG_DIR: &str = "GLIDE_LOG_DIR";

/// Wraps [RollingFileAppender] to defer initialization until logging is required,
/// allowing [init] to disable file logging on read-only filesystems.
/// This is needed because [RollingFileAppender] tries to create the log directory on initialization.
struct LazyRollingFileAppender {
    file_appender: OnceCell<RollingFileAppender>,
    rotation: Rotation,
    directory: PathBuf,
    filename_prefix: PathBuf,
}

impl LazyRollingFileAppender {
    fn new(
        rotation: Rotation,
        directory: impl AsRef<Path>,
        filename_prefix: impl AsRef<Path>,
    ) -> LazyRollingFileAppender {
        LazyRollingFileAppender {
            file_appender: OnceCell::new(),
            rotation,
            directory: directory.as_ref().to_path_buf(),
            filename_prefix: filename_prefix.as_ref().to_path_buf(),
        }
    }
}

impl<'a> tracing_subscriber::fmt::writer::MakeWriter<'a> for LazyRollingFileAppender {
    type Writer = RollingWriter<'a>;
    fn make_writer(&'a self) -> Self::Writer {
        let file_appender = self.file_appender.get_or_init(|| {
            RollingFileAppender::new(
                self.rotation.clone(),
                self.directory.clone(),
                self.filename_prefix.clone(),
            )
        });
        file_appender.make_writer()
    }
}

#[derive(Debug)]
pub enum Level {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
    Trace = 4,
    Off = 5,
}

#[derive(Debug, Clone)]
pub struct StructuredField {
    key: &'static str,
    value: JsonValue,
}

pub fn structured_field<Value: Serialize>(key: &'static str, value: Value) -> StructuredField {
    StructuredField {
        key,
        value: serde_json::to_value(value).unwrap_or(JsonValue::Null),
    }
}

#[macro_export]
macro_rules! structured_fields {
    ($($key:literal => $value:expr),* $(,)?) => {
        &[$($crate::structured_field($key, $value)),*]
    };
}

fn structured_payload(log_identifier: &str, fields: &[StructuredField]) -> String {
    let mut payload = JsonMap::with_capacity(fields.len() + 2);
    payload.insert("glide_structured".to_string(), JsonValue::Bool(true));
    payload.insert(
        "glide_event".to_string(),
        JsonValue::String(log_identifier.to_string()),
    );
    for field in fields {
        payload.insert(field.key.to_string(), field.value.clone());
    }
    serde_json::to_string(&payload).unwrap_or_else(|_| {
        "{\"glide_structured\":true,\"glide_event\":\"serialization_failed\"}".to_string()
    })
}

fn ensure_logger_exists() {
    if INITIATE_ONCE.init_once.get().is_none() {
        init(Some(Level::Warn), None);
    };
}

impl Level {
    fn to_filter(&self) -> filter::LevelFilter {
        match self {
            Level::Trace => LevelFilter::TRACE,
            Level::Debug => LevelFilter::DEBUG,
            Level::Info => LevelFilter::INFO,
            Level::Warn => LevelFilter::WARN,
            Level::Error => LevelFilter::ERROR,
            Level::Off => LevelFilter::OFF,
        }
    }
}

/// Attempt to read a directory path from an environment variable. If the environment variable `envname` exists
/// and contains a valid path - this function will create and return that path. In any case of failure,
/// this method returns `None` (e.g. the environment variable exists but contains an empty path etc)
pub fn create_directory_from_env(envname: &str) -> Option<String> {
    let Ok(dirpath) = std::env::var(envname) else {
        return None;
    };

    if dirpath.trim().is_empty() || std::fs::create_dir_all(&dirpath).is_err() {
        return None;
    }

    Some(dirpath)
}

// Initialize the global logger to error level on the first call only
// In any of the calls to the function, including the first - resetting the existence loggers to the new setting
// provided by using the global reloadable handle
// The logger will save only logs of the given level or above.
pub fn init(minimal_level: Option<Level>, file_name: Option<&str>) -> Level {
    let level = minimal_level.unwrap_or(Level::Warn);
    let level_filter = level.to_filter();
    let reloads = INITIATE_ONCE.init_once.get_or_init(|| {
        let stdout_fmt = tracing_subscriber::fmt::layer()
            .with_ansi(true)
            .with_filter(LevelFilter::OFF);

        let (stdout_layer, stdout_reload) = reload::Layer::new(stdout_fmt);

        // Check if the environment variable GLIDE_LOG is set
        let logs_dir =
            create_directory_from_env(ENV_GLIDE_LOG_DIR).unwrap_or(FILE_DIRECTORY.to_string());
        let file_appender = LazyRollingFileAppender::new(
            Rotation::HOURLY,
            logs_dir,
            file_name.unwrap_or("output.log"),
        );

        let file_fmt = tracing_subscriber::fmt::layer()
            .with_writer(file_appender)
            .with_filter(LevelFilter::OFF);
        let (file_layer, file_reload) = reload::Layer::new(file_fmt);

        // If user has set the environment variable "RUST_LOG" with a valid log verbosity, use it
        let log_level = if let Ok(level) = std::env::var("RUST_LOG") {
            let trace_level = tracing::Level::from_str(&level).unwrap_or(tracing::Level::TRACE);
            LevelFilter::from(trace_level)
        } else {
            LevelFilter::TRACE
        };

        // Enable logging only from allowed crates
        let targets_filter = filter::Targets::new()
            .with_target("glide", log_level)
            .with_target("redis", log_level)
            .with_target("logger_core", log_level)
            .with_target(std::env!("CARGO_PKG_NAME"), log_level);

        tracing_subscriber::registry()
            .with(stdout_layer)
            .with(file_layer)
            .with(targets_filter)
            .init();

        let reloads: Reloads = Reloads {
            console_reload: RwLock::new(stdout_reload),
            file_reload: RwLock::new(file_reload),
        };
        reloads
    });

    match file_name {
        None => {
            let _ = reloads
                .console_reload
                .write()
                .expect("error reloading stdout")
                .modify(|layer| *layer.filter_mut() = level_filter);
            let _ = reloads
                .file_reload
                .write()
                .expect("error reloading file appender")
                .modify(|layer| {
                    *layer.filter_mut() = LevelFilter::OFF;
                });
        }
        Some(file) => {
            // Check if the environment variable GLIDE_LOG is set
            let logs_dir =
                create_directory_from_env(ENV_GLIDE_LOG_DIR).unwrap_or(FILE_DIRECTORY.to_string());
            let file_appender = LazyRollingFileAppender::new(Rotation::HOURLY, logs_dir, file);
            let _ = reloads
                .file_reload
                .write()
                .expect("error reloading file appender")
                .modify(|layer| {
                    *layer.filter_mut() = level_filter;
                    *layer.inner_mut().writer_mut() = file_appender;
                });
            let _ = reloads
                .console_reload
                .write()
                .expect("error reloading stdout")
                .modify(|layer| *layer.filter_mut() = LevelFilter::OFF);
        }
    };
    level
}

macro_rules! create_log {
    ($name:ident, $uppercase_level:tt) => {
        pub fn $name<Message: AsRef<str>, Identifier: AsRef<str>>(
            log_identifier: Identifier,
            message: Message,
        ) {
            ensure_logger_exists();
            let message_ref = message.as_ref();
            let identifier_ref = log_identifier.as_ref();
            event!(
                tracing::Level::$uppercase_level,
                "{identifier_ref} - {message_ref}"
            )
        }
    };
}

create_log!(log_trace, TRACE);
create_log!(log_debug, DEBUG);
create_log!(log_info, INFO);
create_log!(log_warn, WARN);
create_log!(log_error, ERROR);

pub fn log_structured<Identifier: AsRef<str>>(
    log_level: Level,
    log_identifier: Identifier,
    fields: &[StructuredField],
) {
    ensure_logger_exists();
    let payload = structured_payload(log_identifier.as_ref(), fields);
    match log_level {
        Level::Debug => event!(tracing::Level::DEBUG, "{payload}"),
        Level::Trace => event!(tracing::Level::TRACE, "{payload}"),
        Level::Info => event!(tracing::Level::INFO, "{payload}"),
        Level::Warn => event!(tracing::Level::WARN, "{payload}"),
        Level::Error => event!(tracing::Level::ERROR, "{payload}"),
        Level::Off => (),
    }
}

// Logs the given log, with log_identifier and log level prefixed. If the given log level is below the threshold of given when the logger was initialized, the log will be ignored.
// log_identifier should be used to add context to a log, and make it easier to connect it to other relevant logs. For example, it can be used to pass a task identifier.
// If this is called before a logger was initialized the log will not be registered.
// If logger doesn't exist, create the default
pub fn log<Message: AsRef<str>, Identifier: AsRef<str>>(
    log_level: Level,
    log_identifier: Identifier,
    message: Message,
) {
    match log_level {
        Level::Debug => log_debug(log_identifier, message),
        Level::Trace => log_trace(log_identifier, message),
        Level::Info => log_info(log_identifier, message),
        Level::Warn => log_warn(log_identifier, message),
        Level::Error => log_error(log_identifier, message),
        Level::Off => (),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_directory_from_env() {
        let dir_path = format!("{}/glide-logs", std::env::temp_dir().display());
        // Case 1: try to create an already existing folder
        // make sure we are starting fresh
        let _ = std::fs::remove_dir_all(&dir_path);
        // Create the directory
        assert!(std::fs::create_dir_all(&dir_path).is_ok());

        unsafe { std::env::set_var(ENV_GLIDE_LOG_DIR, &dir_path) };
        assert!(create_directory_from_env(ENV_GLIDE_LOG_DIR).is_some());
        assert!(std::fs::metadata(&dir_path).is_ok());

        // Case 2: try to create a new folder (i.e. the folder does not already exist)
        let _ = std::fs::remove_dir_all(&dir_path);

        // Create the directory
        assert!(std::fs::create_dir_all(&dir_path).is_ok());
        assert!(std::fs::metadata(&dir_path).is_ok());

        unsafe { std::env::set_var(ENV_GLIDE_LOG_DIR, &dir_path) };
        assert!(create_directory_from_env(ENV_GLIDE_LOG_DIR).is_some());

        // make sure we are starting fresh
        let _ = std::fs::remove_dir_all(&dir_path);

        // Case 3: empty variable is not acceptable
        unsafe { std::env::set_var(ENV_GLIDE_LOG_DIR, "") };
        assert!(create_directory_from_env(ENV_GLIDE_LOG_DIR).is_none());
    }

    #[test]
    fn test_structured_payload_escapes_values() {
        let payload = structured_payload(
            "command\"event",
            &[
                structured_field("callback_id", 42_i64),
                structured_field("command", "GET\nkey"),
                structured_field("success", false),
            ],
        );

        let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(payload["glide_structured"], true);
        assert_eq!(payload["glide_event"], "command\"event");
        assert_eq!(payload["callback_id"], 42);
        assert_eq!(payload["command"], "GET\nkey");
        assert_eq!(payload["success"], false);
    }
}
