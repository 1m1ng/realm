pub mod cmd;
pub mod conf;
pub mod consts;
pub mod process;

#[cfg(all(unix, feature = "control"))]
pub mod control;

pub use realm_core as core;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const ENV_CONFIG: &str = "REALM_CONF";
pub const ENV_CONTROL_SOCKET: &str = "REALM_CONTROL_SOCKET";
