//! Framing codecs and byte I/O helpers.
//!
//! The core protocol deals in whole JSON messages; a *transport* is responsible
//! for delimiting those messages on a byte stream. This module provides the
//! framings needed for non-HTTP peers, such as a JSON-RPC service reachable
//! over a Unix domain socket or a raw TCP stream.
//!
//! Framings are gated behind the `transport` feature, with `netstring` and
//! `newline` selectable individually. Async I/O helpers require `tokio`.

use crate::error::TransportError;

/// A message framing: how to delimit JSON messages on a byte stream.
///
/// Implementations are sans-I/O: they encode into and decode from in-memory
/// buffers so the same codec works over sync `Read`/`Write`, async streams, or
/// anything else. I/O drivers live alongside under the `tokio` feature.
///
/// ```
/// use jasonrpc::transport::{Framing, Newline};
///
/// let f = Newline;
/// let mut buf = Vec::new();
///
/// // Encode adds a \n terminator.
/// f.encode(b"hello", &mut buf);
/// assert_eq!(buf, b"hello\n");
///
/// // Decode strips the \n and returns the payload.
/// let (msg, consumed) = f.decode(&buf).unwrap().unwrap();
/// assert_eq!(msg, b"hello");
/// assert_eq!(consumed, 6); // "hello\n" = 6 bytes consumed
/// ```
pub trait Framing {
    /// Encode one message payload into `dst`, adding whatever framing overhead
    /// the codec requires.
    fn encode(&self, payload: &[u8], dst: &mut Vec<u8>);

    /// Attempt to decode one complete message from the front of `src`.
    ///
    /// Returns `Ok(Some((message, consumed)))` when a full frame is available,
    /// where `consumed` is the number of bytes the caller should drain from the
    /// front of `src`. Returns `Ok(None)` when more bytes are needed.
    ///
    /// # Errors
    ///
    /// Returns a [`TransportError`] if the framing is malformed.
    fn decode(&self, src: &[u8]) -> Result<Option<(Vec<u8>, usize)>, TransportError>;

    /// When `true`, the I/O driver treats EOF as a valid frame boundary:
    /// whatever is buffered becomes the next message. This supports clients
    /// that delimit requests by half-closing the connection (`shutdown SHUT_WR`)
    /// rather than using an in-band framing marker.
    ///
    /// Defaults to `false`: EOF with a partial frame is an error.
    fn take_on_eof(&self) -> bool {
        false
    }
}

/// Newline-delimited framing: each message is followed by a single `\n`.
///
/// Simple and human-friendly; requires that messages never contain a raw
/// newline (compact JSON satisfies this). Good for interactive control sockets.
///
/// Also tolerant of clients that half-close the write side
/// (`shutdown(SHUT_WR)`) without a trailing newline: when EOF arrives and
/// there is data in the buffer, it is delivered as a complete message. This
/// means a single connection can support both multi-message `\n`-delimited
/// sessions and one-shot clients that use the socket lifecycle as a frame.
#[cfg(feature = "newline")]
#[derive(Clone, Copy, Debug, Default)]
pub struct Newline;

#[cfg(feature = "newline")]
impl Framing for Newline {
    fn encode(&self, payload: &[u8], dst: &mut Vec<u8>) {
        dst.reserve(payload.len() + 1);
        dst.extend_from_slice(payload);
        dst.push(b'\n');
    }

    fn decode(&self, src: &[u8]) -> Result<Option<(Vec<u8>, usize)>, TransportError> {
        if let Some(idx) = memchr::memchr(b'\n', src) {
            Ok(Some((src[..idx].to_vec(), idx + 1)))
        } else {
            Ok(None)
        }
    }

    fn take_on_eof(&self) -> bool {
        true
    }
}

/// Netstring framing: `<len>:<payload>,` (see <https://cr.yp.to/proto/netstrings.txt>).
///
/// Length-prefixed and self-delimiting, so payloads may contain any bytes.
/// A good default for machine-to-machine UDS links.
#[cfg(feature = "netstring")]
#[derive(Clone, Copy, Debug, Default)]
pub struct Netstring;

#[cfg(feature = "netstring")]
impl Framing for Netstring {
    fn encode(&self, payload: &[u8], dst: &mut Vec<u8>) {
        let mut itoa_buf = itoa::Buffer::new();
        let header = itoa_buf.format(payload.len());
        dst.reserve(header.len() + payload.len() + 2);
        dst.extend_from_slice(header.as_bytes());
        dst.push(b':');
        dst.extend_from_slice(payload);
        dst.push(b',');
    }

    fn decode(&self, src: &[u8]) -> Result<Option<(Vec<u8>, usize)>, TransportError> {
        // Scalar scan: on well-formed input the colon is only a few bytes away.
        let Some(colon) = src.iter().position(|&b| b == b':') else {
            // Guard against an unbounded/garbage length prefix.
            if src.len() > 20 {
                return Err(TransportError::Frame(
                    "netstring length prefix too long".into(),
                ));
            }
            return Ok(None);
        };

        let len: usize = std::str::from_utf8(&src[..colon])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| TransportError::Frame("invalid netstring length".into()))?;

        // Guard against overflow on 32-bit targets where a huge (but <=20 digit)
        // length could wrap `colon + 1 + len`.
        let end = colon
            .checked_add(1)
            .and_then(|v| v.checked_add(len))
            .ok_or_else(|| TransportError::Frame("netstring length overflows usize".into()))?;
        if src.len() <= end {
            return Ok(None); // need the payload plus the trailing comma
        }
        if src[end] != b',' {
            return Err(TransportError::Frame(
                "netstring missing trailing comma".into(),
            ));
        }

        Ok(Some((src[colon + 1..end].to_vec(), end + 1)))
    }

    fn take_on_eof(&self) -> bool {
        false
    }
}

/// Tokio-based async I/O driver over a [`Framing`] codec.
#[cfg(feature = "tokio")]
pub mod io {
    use std::time::Duration;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    use super::Framing;
    use crate::error::TransportError;

    /// Default maximum frame size: 16 MiB.
    ///
    /// JSON-RPC requests larger than this are almost certainly an attack or a
    /// bug. Set to 0 to disable the limit (not recommended for network-facing
    /// servers).
    pub const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

    /// Shared decode/read-buffer machinery for [`FramedReader`] and
    /// [`FramedConn`].
    ///
    /// Holds the pending byte buffer plus the read timeout and frame-size cap,
    /// and knows how to pull one complete frame from any [`AsyncRead`] given a
    /// [`Framing`]. Keeping this in one place means the reader and the duplex
    /// connection share exactly the same framing, EOF, timeout, and size-limit
    /// behavior rather than duplicating it.
    struct ReadCore {
        read_buf: Vec<u8>,
        read_timeout: Option<Duration>,
        max_frame_size: usize,
    }

    impl ReadCore {
        fn new() -> Self {
            Self {
                read_buf: Vec::with_capacity(CHUNK_SIZE),
                read_timeout: None,
                max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            }
        }

        /// Read a single complete payload, or `None` at clean end-of-stream.
        async fn recv<R, F>(
            &mut self,
            inner: &mut R,
            framing: &F,
        ) -> Result<Option<Vec<u8>>, TransportError>
        where
            R: AsyncRead + Unpin,
            F: Framing,
        {
            loop {
                if let Some((msg, consumed)) = framing.decode(&self.read_buf)? {
                    self.read_buf.drain(..consumed);
                    return Ok(Some(msg));
                }

                let n = self.fill(inner).await?;
                if n == 0 {
                    return if framing.take_on_eof() {
                        if self.read_buf.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some(std::mem::take(&mut self.read_buf)))
                        }
                    } else if self.read_buf.is_empty() {
                        Ok(None)
                    } else {
                        Err(TransportError::Frame("stream closed mid-frame".into()))
                    };
                }
            }
        }

        /// Read one chunk directly into the tail of `read_buf` (no intermediate
        /// stack buffer or extra copy), enforcing the frame-size cap. Returns
        /// the number of bytes read (`0` = EOF).
        async fn fill<R>(&mut self, inner: &mut R) -> Result<usize, TransportError>
        where
            R: AsyncRead + Unpin,
        {
            let start = self.read_buf.len();
            if self.max_frame_size > 0 && start >= self.max_frame_size {
                return Err(TransportError::Frame(format!(
                    "frame exceeds max size of {} bytes",
                    self.max_frame_size
                )));
            }

            // Grow to receive up to CHUNK_SIZE bytes into the tail, read into
            // that slice, then shrink back to what actually arrived.
            let timeout = self.read_timeout;
            self.read_buf.resize(start + CHUNK_SIZE, 0);
            let read_result = {
                let dst = &mut self.read_buf[start..];
                match timeout {
                    Some(to) => tokio::time::timeout(to, inner.read(dst))
                        .await
                        .map_err(|_| TransportError::Frame(format!("read timed out after {to:?}")))?
                        .map_err(TransportError::from),
                    None => inner.read(dst).await.map_err(TransportError::from),
                }
            };
            let n = match read_result {
                Ok(n) => n,
                Err(e) => {
                    self.read_buf.truncate(start);
                    return Err(e);
                }
            };
            self.read_buf.truncate(start + n);

            if self.max_frame_size > 0 && self.read_buf.len() > self.max_frame_size {
                return Err(TransportError::Frame(format!(
                    "frame exceeds max size of {} bytes",
                    self.max_frame_size
                )));
            }
            Ok(n)
        }
    }

    /// Size of each stream read into the buffer tail.
    const CHUNK_SIZE: usize = 4096;

    /// The read half of a framed connection: decodes whole payloads from a
    /// byte stream according to a [`Framing`].
    ///
    /// Split out from [`FramedConn`] so a reader can live in its own task (e.g.
    /// the multiplexing client's background reader) while a [`FramedWriter`]
    /// drives the write half concurrently.
    pub struct FramedReader<R, F> {
        inner: R,
        framing: F,
        core: ReadCore,
    }

    impl<R, F> FramedReader<R, F>
    where
        R: AsyncRead + Unpin,
        F: Framing,
    {
        /// Wrap a read half with the given framing.
        ///
        /// Uses [`DEFAULT_MAX_FRAME_SIZE`] (16 MiB) and no per-read timeout
        /// by default. Use [`with_read_timeout`](Self::with_read_timeout) and
        /// [`with_max_frame_size`](Self::with_max_frame_size) to tune.
        pub fn new(inner: R, framing: F) -> Self {
            Self {
                inner,
                framing,
                core: ReadCore::new(),
            }
        }

        /// Set a per-read timeout. When set, each `read()` call on the
        /// underlying stream will be cancelled if it takes longer than
        /// `timeout`. This prevents slowloris-style trickle attacks where a
        /// client sends one byte at a time to keep a connection alive
        /// indefinitely.
        #[must_use]
        pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
            self.core.read_timeout = Some(timeout);
            self
        }

        /// Set the maximum frame size in bytes. Frames larger than this will
        /// be rejected with a [`TransportError::Frame`] error.
        ///
        /// Set to 0 to disable the limit (every read will buffer until EOF or
        /// a complete frame — dangerous for network-facing services).
        #[must_use]
        pub fn with_max_frame_size(mut self, limit: usize) -> Self {
            self.core.max_frame_size = limit;
            self
        }

        /// Read a single complete payload, or `None` at clean end-of-stream.
        ///
        /// # Errors
        ///
        /// Returns a [`TransportError`] on I/O errors, framing violations,
        /// read timeout, or if the frame exceeds [`max_frame_size`](Self::with_max_frame_size).
        pub async fn recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            self.core.recv(&mut self.inner, &self.framing).await
        }
    }

    /// The write half of a framed connection: frames and writes whole payloads.
    pub struct FramedWriter<W, F> {
        inner: W,
        framing: F,
        write_buf: Vec<u8>,
    }

    impl<W, F> FramedWriter<W, F>
    where
        W: AsyncWrite + Unpin,
        F: Framing,
    {
        /// Wrap a write half with the given framing.
        pub fn new(inner: W, framing: F) -> Self {
            Self {
                inner,
                framing,
                write_buf: Vec::new(),
            }
        }

        /// Frame and write a single payload, flushing the stream.
        ///
        /// # Errors
        ///
        /// Returns a [`TransportError`] on I/O errors.
        pub async fn send(&mut self, payload: &[u8]) -> Result<(), TransportError> {
            self.write_buf.clear();
            self.framing.encode(payload, &mut self.write_buf);
            self.inner.write_all(&self.write_buf).await?;
            self.inner.flush().await?;
            Ok(())
        }
    }

    /// A framed connection over an async read/write stream.
    ///
    /// Wraps any `AsyncRead + AsyncWrite` (a `UnixStream`, `TcpStream`, or the
    /// duplex halves of one) and reads/writes whole JSON payloads according to
    /// the supplied [`Framing`]. For concurrent read/write from separate tasks,
    /// use [`FramedReader`] / [`FramedWriter`] over the split halves instead.
    pub struct FramedConn<T, F> {
        inner: T,
        framing: F,
        core: ReadCore,
        write_buf: Vec<u8>,
    }

    impl<T, F> FramedConn<T, F>
    where
        T: AsyncRead + AsyncWrite + Unpin,
        F: Framing,
    {
        /// Wrap a stream with the given framing.
        ///
        /// Uses [`DEFAULT_MAX_FRAME_SIZE`] (16 MiB) and no per-read timeout
        /// by default.
        pub fn new(inner: T, framing: F) -> Self {
            Self {
                inner,
                framing,
                core: ReadCore::new(),
                write_buf: Vec::new(),
            }
        }

        /// Set a per-read timeout. See [`FramedReader::with_read_timeout`].
        #[must_use]
        pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
            self.core.read_timeout = Some(timeout);
            self
        }

        /// Set the maximum frame size in bytes. See [`FramedReader::with_max_frame_size`].
        #[must_use]
        pub fn with_max_frame_size(mut self, limit: usize) -> Self {
            self.core.max_frame_size = limit;
            self
        }

        /// Frame and write a single payload, flushing the stream.
        ///
        /// # Errors
        ///
        /// Returns a [`TransportError`] on I/O errors.
        pub async fn send(&mut self, payload: &[u8]) -> Result<(), TransportError> {
            self.write_buf.clear();
            self.framing.encode(payload, &mut self.write_buf);
            self.inner.write_all(&self.write_buf).await?;
            self.inner.flush().await?;
            Ok(())
        }

        /// Read a single complete payload, or `None` at clean end-of-stream.
        ///
        /// # Errors
        ///
        /// Returns a [`TransportError`] on I/O errors, framing violations,
        /// read timeout, or if the frame exceeds the configured max size.
        pub async fn recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            self.core.recv(&mut self.inner, &self.framing).await
        }
    }
}

#[cfg(all(test, feature = "netstring"))]
mod netstring_tests {
    use super::*;

    #[test]
    fn round_trip() {
        let f = Netstring;
        let mut buf = Vec::new();
        f.encode(b"hello", &mut buf);
        assert_eq!(buf, b"5:hello,");
        let (msg, consumed) = f.decode(&buf).unwrap().unwrap();
        assert_eq!(msg, b"hello");
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn partial_returns_none() {
        let f = Netstring;
        assert!(f.decode(b"5:hel").unwrap().is_none());
    }

    #[test]
    fn bad_terminator_errors() {
        let f = Netstring;
        assert!(f.decode(b"5:hello!").is_err());
    }

    #[test]
    fn take_on_eof_is_false() {
        assert!(!Netstring.take_on_eof());
    }
}

#[cfg(all(test, feature = "newline"))]
mod newline_tests {
    use super::*;

    #[test]
    fn round_trip() {
        let f = Newline;
        let mut buf = Vec::new();
        f.encode(b"{}", &mut buf);
        assert_eq!(buf, b"{}\n");
        let (msg, consumed) = f.decode(&buf).unwrap().unwrap();
        assert_eq!(msg, b"{}");
        assert_eq!(consumed, 3);
    }

    #[test]
    fn encode_appends_newline() {
        let f = Newline;
        let mut dst = Vec::new();
        f.encode(b"hello", &mut dst);
        assert_eq!(dst, b"hello\n");
    }

    #[test]
    fn decode_no_newline_returns_none() {
        let f = Newline;
        assert!(f.decode(b"no_newline_here").unwrap().is_none());
    }

    #[test]
    fn take_on_eof_is_true() {
        assert!(Newline.take_on_eof());
    }
}

#[cfg(all(test, feature = "tokio", feature = "newline"))]
mod newline_framed_io_tests {
    use super::*;
    use io::FramedReader;
    use std::io::Cursor;

    #[tokio::test]
    async fn reader_delivers_on_eof_without_newline() {
        // Simulates a C client: write() + shutdown(SHUT_WR) — no trailing \n.
        let data = Cursor::new(b"{\"jsonrpc\":\"2.0\"}");
        let mut reader = FramedReader::new(data, Newline);
        let msg = reader.recv().await.unwrap();
        assert_eq!(msg.as_deref(), Some(b"{\"jsonrpc\":\"2.0\"}".as_slice()));

        // Next recv: clean EOF, nothing buffered.
        let done = reader.recv().await.unwrap();
        assert!(done.is_none());
    }

    #[tokio::test]
    async fn reader_empty_eof_returns_none() {
        let data = Cursor::new(b"");
        let mut reader = FramedReader::new(data, Newline);
        let msg = reader.recv().await.unwrap();
        assert!(msg.is_none());
    }

    #[tokio::test]
    async fn reader_multiple_newline_frames_then_eof() {
        // Two newline-delimited frames, then a third with no trailing \n.
        let data = Cursor::new(b"{}\n{\"a\":1}\n{\"b\":2}");
        let mut reader = FramedReader::new(data, Newline);

        let msg1 = reader.recv().await.unwrap();
        assert_eq!(msg1.as_deref(), Some(b"{}".as_slice()));

        let msg2 = reader.recv().await.unwrap();
        assert_eq!(msg2.as_deref(), Some(b"{\"a\":1}".as_slice()));

        let msg3 = reader.recv().await.unwrap();
        assert_eq!(msg3.as_deref(), Some(b"{\"b\":2}".as_slice()));

        let done = reader.recv().await.unwrap();
        assert!(done.is_none());
    }
}

#[cfg(all(test, feature = "tokio", feature = "newline"))]
mod framed_io_tests {
    use super::*;
    use io::FramedWriter;

    #[tokio::test]
    async fn framed_writer_send() {
        // Sink that captures the written bytes.
        let mut buf = Vec::<u8>::new();
        let mut writer = FramedWriter::new(&mut buf, Newline);
        writer.send(b"hello").await.unwrap();
        assert_eq!(buf, b"hello\n");
    }

    #[tokio::test]
    async fn framed_reader_clean_eof() {
        use io::FramedReader;
        use std::io::Cursor;

        // A complete frame followed by nothing (eof on next read).
        let data = Cursor::new(b"{}\n");
        let mut reader = FramedReader::new(data, Newline);
        let msg = reader.recv().await.unwrap();
        assert_eq!(msg, Some(b"{}".to_vec()));

        // The next recv should return None (clean EOF).
        let done = reader.recv().await.unwrap();
        assert!(done.is_none());
    }

    #[tokio::test]
    async fn read_timeout_kills_slow_read() {
        use io::FramedReader;
        use std::time::Duration;

        // A mock stream that yields no data and just idles.
        struct SlowStream;
        impl tokio::io::AsyncRead for SlowStream {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                _buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Pending
            }
        }

        let mut reader =
            FramedReader::new(SlowStream, Newline).with_read_timeout(Duration::from_millis(10));
        let err = reader.recv().await.unwrap_err();
        assert!(
            matches!(err, TransportError::Frame(ref m) if m.contains("timed out")),
            "expected timeout, got {err:?}"
        );
    }

    #[tokio::test]
    async fn max_frame_size_rejects_oversized() {
        use io::FramedReader;
        use std::io::Cursor;

        // 100 bytes of 'x' — exceeds the 50 byte limit.
        let data: Vec<u8> = (0..100).map(|_| b'x').collect();
        let mut reader = FramedReader::new(Cursor::new(data), Newline).with_max_frame_size(50);
        let err = reader.recv().await.unwrap_err();
        assert!(
            matches!(err, TransportError::Frame(ref m) if m.contains("exceeds max size")),
            "expected size error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn reader_reassembles_frame_across_many_small_reads() {
        use io::FramedReader;

        // A stream that yields one byte per poll, so a frame larger than a
        // single read must be reassembled in the buffer tail across many fills.
        struct Trickle {
            data: Vec<u8>,
            pos: usize,
        }
        impl tokio::io::AsyncRead for Trickle {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                if self.pos < self.data.len() && buf.remaining() > 0 {
                    let b = self.data[self.pos];
                    self.pos += 1;
                    buf.put_slice(&[b]);
                }
                std::task::Poll::Ready(Ok(()))
            }
        }

        // A payload bigger than one chunk would be ideal, but even a modest one
        // exercises the multi-read reassembly path with a 1-byte trickle.
        let payload = br#"{"jsonrpc":"2.0","method":"m","params":[1,2,3,4,5],"id":1}"#;
        let mut data = Vec::new();
        Newline.encode(payload, &mut data);
        let stream = Trickle { data, pos: 0 };

        let mut reader = FramedReader::new(stream, Newline);
        let msg = reader.recv().await.unwrap();
        assert_eq!(msg.as_deref(), Some(payload.as_slice()));
    }
}
