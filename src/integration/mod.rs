//! Optional integrations with the wider Rust ecosystem.
//!
//! Each integration is behind its own feature flag and is a thin adapter over
//! the transport-agnostic [`server`](crate::server) / [`client`](crate::client)
//! cores. They add convenience, never protocol behavior.

/// Default maximum HTTP request body size: 16 MiB.
#[cfg(any(feature = "hyper", feature = "tower"))]
pub const DEFAULT_MAX_REQUEST_BODY_SIZE: usize = 16 * 1024 * 1024;

// HTTP-library-neutral dispatch logic shared by the hyper and tower adapters.
#[cfg(any(feature = "hyper", feature = "tower"))]
pub(super) mod http;

#[cfg(feature = "hyper")]
pub mod hyper;

#[cfg(feature = "tower")]
pub mod tower;

#[cfg(feature = "websocket")]
pub mod websocket;
