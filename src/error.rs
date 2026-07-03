//! Crate-level error types.
//!
//! These are Rust `Error`s for library-level failures. They are distinct from
//! the JSON-RPC [`Error`](crate::protocol::Error) *protocol* object, which is a
//! value that travels on the wire. Don't conflate the two.

#[cfg(feature = "client")]
use crate::protocol::Error as RpcError;

/// Errors from framing / byte I/O in the transport layer.
///
/// ```
/// use jasonrpc::error::TransportError;
///
/// let io_err = TransportError::Io(std::io::Error::other("broken pipe"));
/// let frame_err = TransportError::Frame("bad length prefix".into());
///
/// assert!(io_err.to_string().contains("io error"));
/// assert!(frame_err.to_string().contains("framing error"));
/// ```
#[cfg(feature = "transport")]
#[derive(Debug)]
#[non_exhaustive]
pub enum TransportError {
    /// A framing violation (bad length prefix, missing terminator, etc.).
    Frame(String),
    /// Underlying I/O error.
    Io(std::io::Error),
}

#[cfg(feature = "transport")]
impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Frame(m) => write!(f, "framing error: {m}"),
            TransportError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

#[cfg(feature = "transport")]
impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TransportError::Io(e) => Some(e),
            TransportError::Frame(_) => None,
        }
    }
}

#[cfg(feature = "transport")]
impl From<std::io::Error> for TransportError {
    fn from(e: std::io::Error) -> Self {
        TransportError::Io(e)
    }
}

/// Errors surfaced by the high-level [`Client`](crate::client::Client).
///
/// ```
/// use jasonrpc::error::ClientError;
/// use jasonrpc::json::JsonError;
/// use jasonrpc::protocol::Error as RpcError;
///
/// let proto = ClientError::Protocol(JsonError::new("invalid JSON"));
/// let rpc = ClientError::Rpc(RpcError::method_not_found());
/// let timeout = ClientError::Timeout(std::time::Duration::from_secs(5));
///
/// assert!(proto.to_string().contains("protocol"));
/// assert!(rpc.to_string().contains("rpc error"));
/// assert!(timeout.to_string().contains("timed out"));
/// ```
#[cfg(feature = "client")]
#[derive(Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// (De)serialization or other protocol-level failure.
    Protocol(crate::json::JsonError),
    /// The peer returned a JSON-RPC error object.
    Rpc(RpcError),
    /// The response id did not match the request id.
    IdMismatch,
    /// The response had neither/both of `result` and `error`.
    MalformedResponse,
    /// The underlying transport failed.
    Transport(Box<dyn std::error::Error + Send + Sync>),
    /// The request timed out before a response was received.
    Timeout(std::time::Duration),
}

#[cfg(feature = "client")]
impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Protocol(e) => write!(f, "protocol error: {e}"),
            ClientError::Rpc(e) => write!(f, "rpc error: {e}"),
            ClientError::IdMismatch => write!(f, "response id did not match request id"),
            ClientError::MalformedResponse => {
                write!(f, "response contained neither or both of result/error")
            }
            ClientError::Transport(e) => write!(f, "transport error: {e}"),
            ClientError::Timeout(d) => write!(f, "request timed out after {d:?}"),
        }
    }
}

#[cfg(feature = "client")]
impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ClientError::Protocol(e) => Some(e),
            ClientError::Rpc(e) => Some(e),
            ClientError::Transport(e) => Some(e.as_ref()),
            ClientError::IdMismatch | ClientError::MalformedResponse | ClientError::Timeout(_) => {
                None
            }
        }
    }
}

#[cfg(all(test, feature = "client"))]
mod client_error_tests {
    use super::*;
    use crate::protocol::Error as RpcError;

    #[test]
    fn client_error_display_covers_all_variants() {
        let rpc = RpcError::method_not_found();
        let json_err = crate::json::JsonError("test error".into());
        let transport = std::io::Error::other("io fail");

        assert!(ClientError::Protocol(json_err)
            .to_string()
            .contains("protocol"));
        assert!(ClientError::Rpc(rpc).to_string().contains("rpc error"));
        assert!(ClientError::IdMismatch.to_string().contains("id"));
        assert!(ClientError::MalformedResponse
            .to_string()
            .contains("neither or both"));
        assert!(ClientError::Transport(Box::new(transport))
            .to_string()
            .contains("transport"));
        assert!(ClientError::Timeout(std::time::Duration::from_secs(5))
            .to_string()
            .contains("timed out"));
    }

    fn source_of(e: &dyn std::error::Error) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(e)
    }

    #[test]
    fn client_error_source() {
        let rpc = RpcError::method_not_found();
        let json_err = crate::json::JsonError("test error".into());
        assert!(source_of(&ClientError::Protocol(json_err)).is_some());
        assert!(source_of(&ClientError::Rpc(rpc)).is_some());
        assert!(source_of(&ClientError::IdMismatch).is_none());
        assert!(source_of(&ClientError::MalformedResponse).is_none());
        assert!(source_of(&ClientError::Timeout(std::time::Duration::from_secs(1))).is_none());
        let io = std::io::Error::other("io");
        assert!(source_of(&ClientError::Transport(Box::new(io))).is_some());
    }

    #[test]
    fn transport_error_display() {
        let frame = TransportError::Frame("bad frame".into());
        assert!(frame.to_string().contains("framing error"));

        let io = TransportError::Io(std::io::Error::other("io"));
        assert!(io.to_string().contains("io error"));
    }
}
