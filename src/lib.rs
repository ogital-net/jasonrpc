#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

pub mod json;
pub mod protocol;

#[cfg(any(feature = "client", feature = "transport"))]
pub mod error;

#[cfg(feature = "server")]
pub mod server;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "transport")]
pub mod transport;

#[cfg(any(feature = "hyper", feature = "tower", feature = "websocket"))]
pub mod integration;

// Convenient top-level re-exports of the always-on protocol types.
pub use protocol::{codes, Error, Id, Request, Response, Version};
