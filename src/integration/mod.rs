//! Optional integrations with the wider Rust ecosystem.
//!
//! Each integration is behind its own feature flag and is a thin adapter over
//! the transport-agnostic [`server`](crate::server) / [`client`](crate::client)
//! cores. They add convenience, never protocol behavior.

// HTTP-library-neutral dispatch logic shared by the hyper and tower adapters.
#[cfg(any(feature = "hyper", feature = "tower"))]
pub(super) mod http;

#[cfg(feature = "hyper")]
pub mod hyper;

#[cfg(feature = "tower")]
pub mod tower;
