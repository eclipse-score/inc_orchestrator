// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

pub mod prelude;

use std::fmt::Write;
use std::fs::File;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs};
use tracing::level_filters::LevelFilter;
use tracing::{span, Level, Span};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_perfetto_sdk_layer::{self as layer, NativeLayer};
use tracing_perfetto_sdk_schema as schema;
use tracing_perfetto_sdk_schema::trace_config;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::format;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

const TRACE_OUTDIR_ENV_VAR: &str = "TRACE_OUTDIR";

#[derive(Debug, Clone, Copy)]
pub enum LogMode {
    Logging,
    Tracing,
}

#[derive(Debug, Clone, Copy)]
pub enum TraceScope {
    ///
    /// Logs events from App and if `traced` is running also logs system events (if not, no kernel traces will be there)
    ///
    AppScope,

    ///
    /// Logs app event to `traced` and the events need to be dumped by `perfetto` tool. This is useful once you want to trace multiple apps
    ///
    SystemScope,
}

pub struct TracingLibrary {
    log_level: Level,
    enable_tracing: Option<TraceScope>,
    enable_logging: bool,

    local_tracer_guard: Option<WorkerGuard>,
}

pub struct TracingLibraryBuilder {
    log_level: Level,
    enable_tracing: Option<TraceScope>,
    enable_logging: bool,
}

impl Default for TracingLibraryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TracingLibraryBuilder {
    pub fn new() -> Self {
        Self {
            log_level: Level::INFO,
            enable_tracing: None,
            enable_logging: false,
        }
    }

    pub fn global_log_level(mut self, level: Level) -> Self {
        self.log_level = level;
        self
    }

    ///
    /// Enables tracing in given mode
    ///
    pub fn enable_tracing(mut self, scope: TraceScope) -> Self {
        self.enable_tracing = Some(scope);
        self
    }

    ///
    /// Enables logging
    ///
    pub fn enable_logging(mut self, enable: bool) -> Self {
        self.enable_logging = enable;
        self
    }

    pub fn build(self) -> TracingLibrary {
        TracingLibrary {
            log_level: self.log_level,
            enable_tracing: self.enable_tracing,
            enable_logging: self.enable_logging,
            local_tracer_guard: None,
        }
    }
}

fn system_trace_config() -> schema::TraceConfig {
    schema::TraceConfig {
        buffers: vec![trace_config::BufferConfig {
            size_kb: Some(20480),
            ..Default::default()
        }],
        data_sources: vec![trace_config::DataSource {
            config: Some(schema::DataSourceConfig {
                name: Some("rust_tracing".into()),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn local_trace_config() -> schema::TraceConfig {
    const FTRACE_EVENTS: [&str; 3] = ["sched_switch", "sched_wakeup", "sched_waking"];
    let ftrace = schema::FtraceConfig {
        ftrace_events: FTRACE_EVENTS.iter().map(|&s| s.to_string()).collect(),
        ..Default::default()
    };

    schema::TraceConfig {
        buffers: vec![trace_config::BufferConfig {
            size_kb: Some(20480),
            ..Default::default()
        }],
        data_sources: vec![
            trace_config::DataSource {
                config: Some(schema::DataSourceConfig {
                    name: Some("rust_tracing".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            trace_config::DataSource {
                config: Some(schema::DataSourceConfig {
                    name: Some("linux.process_stats".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            trace_config::DataSource {
                config: Some(schema::DataSourceConfig {
                    name: Some("linux.perf".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            trace_config::DataSource {
                config: Some(schema::DataSourceConfig {
                    name: Some("linux.ftrace".into()),
                    ftrace_config: Some(ftrace),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

impl TracingLibrary {
    pub fn init_log_trace(&mut self) {
        let registry = tracing_subscriber::Registry::default();

        let mut layers = None;

        let fmt_layer = fmt::layer()
            .with_writer(std::io::stdout)
            .event_format(format::Format::default().with_thread_ids(true))
            .with_span_events(format::FmtSpan::FULL)
            .with_filter(LevelFilter::from_level(self.log_level));

        if self.enable_logging {
            layers = Some(fmt_layer.boxed());
        }

        if let Some(tracing_mode) = self.enable_tracing {
            match tracing_mode {
                TraceScope::AppScope => {
                    // Initialize tracing
                    let file = File::create(self.get_trace_filename()).expect("Unable to create tracing file");
                    let (nb, guard) = tracing_appender::non_blocking(file);
                    self.local_tracer_guard = Some(guard);

                    let perfetto_layer = NativeLayer::from_config(local_trace_config(), nb)
                        .with_enable_system(true)
                        .with_enable_in_process(true)
                        .build()
                        .unwrap();

                    layers = Some(match layers {
                        Some(l) => l.and_then(perfetto_layer).boxed(),
                        None => perfetto_layer.boxed(),
                    })
                }
                TraceScope::SystemScope => {
                    let perfetto_layer = layer::SdkLayer::from_config(system_trace_config(), None)
                        .with_enable_system(true)
                        .build()
                        .unwrap();

                    layers = Some(match layers {
                        Some(l) => l.and_then(perfetto_layer).boxed(),
                        None => perfetto_layer.boxed(),
                    })
                }
            }
        }

        if let Some(layer) = layers {
            tracing::subscriber::set_global_default(registry.with(layer)).unwrap();
        }
    }
    /**
     * @brief This API is used to create a file name for the tracing file.
     */
    fn get_trace_filename(&self) -> PathBuf {
        // Get the current process name
        let process_name = env::current_exe()
            .ok()
            .and_then(|path| path.file_name().map(|name| name.to_string_lossy().into_owned()))
            .unwrap_or_else(|| "unknown_process".to_string());

        // Get the current timestamp
        let start = SystemTime::now();
        let duration = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
        // Format timestamp into seconds
        let seconds = duration.as_secs();
        let date_time = self.format_timestamp(seconds);

        // Set the trace output directory if specified by the env var or /tmp otherwise
        let out_dir = match env::var(TRACE_OUTDIR_ENV_VAR) {
            Ok(dir) => PathBuf::from(dir),
            Err(_) => env::temp_dir(),
        };

        // Create the output directory if not yet exist
        if !out_dir.exists() {
            fs::create_dir(&out_dir).unwrap();
        }

        // Generate the filename
        let filename = format!("{}/trace_{}_{}.pftrace", out_dir.display(), process_name, date_time);
        PathBuf::from(filename)
    }

    /**
     * @brief Formats timestamp as YYYY-MM-DD_HH-MM-SS". This is used for naming the tracing file.
     */
    fn format_timestamp(&self, seconds: u64) -> String {
        let days = seconds / 86400;
        let hours = (seconds % 86400) / 3600;
        let minutes = (seconds % 3600) / 60;
        let seconds = seconds % 60;
        let mut formatted_time = String::new();
        write!(
            &mut formatted_time,
            "{:04}-{:02}-{:02}_{:02}-{:02}-{:02}",
            1970 + (days / 365),
            1 + (days % 365) / 30,
            days % 30 + 1,
            hours,
            minutes,
            seconds
        )
        .unwrap();
        formatted_time
    }

    /**
     * @brief Creates a span for the process/ function which initializes tracing
     * Returns a span which is required to control the life of the span
     * Contraints : The name of the span is hardcoded as span API needs a value that is known at compile time.
     */
    pub fn create_span(&self) -> Span {
        let span = span!(Level::TRACE, "Initial Span");
        span
    }
}
