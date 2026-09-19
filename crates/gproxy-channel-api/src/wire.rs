//! Wire-adjacent primitives shared by the contract and the engine.

use std::borrow::Cow;

use bytes::Bytes;

/// Failures crossing the wire to an upstream.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("connect: {0}")]
    Connect(String),
    #[error("timed out")]
    Timeout,
    #[error("upstream status {0}")]
    Status(u16),
    #[error("stream interrupted: {0}")]
    Interrupted(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Alpn {
    Http1,
    Http2,
    Http3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TlsVersion {
    Tls10,
    Tls11,
    Tls12,
    Tls13,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Http2Setting {
    HeaderTableSize,
    EnablePush,
    MaxConcurrentStreams,
    InitialWindowSize,
    MaxFrameSize,
    MaxHeaderListSize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PseudoHeader {
    Method,
    Scheme,
    Authority,
    Path,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Http2Profile {
    pub enable_push: Option<bool>,
    pub initial_window_size: Option<u32>,
    pub initial_connection_window_size: Option<u32>,
    pub max_frame_size: Option<u32>,
    pub max_header_list_size: Option<u32>,
    pub header_table_size: Option<u32>,
    pub max_concurrent_streams: Option<u32>,
    pub pseudo_header_order: Option<Cow<'static, [PseudoHeader]>>,
    pub settings_order: Option<Cow<'static, [Http2Setting]>>,
}

/// Channel-declared native client fingerprint. Edge hosts ignore optional
/// profiles because their runtimes own the TLS stack; requests marked with
/// [`RequiredClientProfile`] fail instead of silently losing one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ClientProfile {
    pub preset: Option<ClientProfilePreset>,
    pub alpn: Option<Cow<'static, [Alpn]>>,
    pub min_tls_version: Option<TlsVersion>,
    pub max_tls_version: Option<TlsVersion>,
    pub cipher_list: Option<Cow<'static, str>>,
    pub curves_list: Option<Cow<'static, str>>,
    pub sigalgs_list: Option<Cow<'static, str>>,
    pub preserve_tls13_cipher_list: Option<bool>,
    pub grease: Option<bool>,
    pub extension_permutation: Option<Cow<'static, [u16]>>,
    pub http2: Option<Http2Profile>,
}

/// A channel's exportable client defaults. Headers contain only static client
/// metadata; credentials and per-request/session values belong to preparation.
#[derive(Debug, Clone)]
pub struct ClientFingerprint {
    pub id: &'static str,
    pub label: &'static str,
    pub headers: http::HeaderMap,
    pub profile: &'static ClientProfile,
}

impl ClientProfile {
    pub const fn preset(preset: ClientProfilePreset) -> Self {
        Self {
            preset: Some(preset),
            alpn: None,
            min_tls_version: None,
            max_tls_version: None,
            cipher_list: None,
            curves_list: None,
            sigalgs_list: None,
            preserve_tls13_cipher_list: None,
            grease: None,
            extension_permutation: None,
            http2: None,
        }
    }

    pub const fn is_usable(&self) -> bool {
        self.preset.is_some() || self.has_transport_overrides()
    }

    /// Whether anything besides [`ClientProfile::preset`] is set. A preset is a
    /// captured browser fingerprint applied whole, so hosts take it in place of
    /// these fields rather than layering the two: a half-applied capture is a
    /// fingerprint no real client has. Setting both is an authoring mistake,
    /// and this is what a host checks before saying so.
    pub const fn has_transport_overrides(&self) -> bool {
        self.alpn.is_some()
            || self.min_tls_version.is_some()
            || self.max_tls_version.is_some()
            || self.cipher_list.is_some()
            || self.curves_list.is_some()
            || self.sigalgs_list.is_some()
            || self.preserve_tls13_cipher_list.is_some()
            || self.grease.is_some()
            || self.extension_permutation.is_some()
            || self.http2.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientProfilePreset {
    /// Captured desktop Chrome 148 TLS, HTTP/2 and default-header behavior.
    Chrome148,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequiredClientProfile;

/// Response body stream. Zero-copy passthrough is the default path: frames
/// flow as refcounted `Bytes` and are only re-encoded when something must
/// rewrite them.
///
/// The `Send` split is the one language-level tax carried for the wasm
/// target (single-threaded executors; futures there are not `Send`).
#[cfg(not(target_arch = "wasm32"))]
pub type ByteStream =
    std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<Bytes, TransportError>> + Send>>;
#[cfg(target_arch = "wasm32")]
pub type ByteStream =
    std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<Bytes, TransportError>>>>;

/// Stable credential identity. i64 to match relational primary keys;
/// embedders without a database can hand out any distinct values.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct CredentialId(pub i64);

/// `Send` on native, nothing on wasm — the marker that lets one trait
/// definition serve both targets instead of duplicating it under `#[cfg]`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

/// `Sync` on native, unconstrained on single-threaded wasm. The core's
/// returned stream owns shared host services across settlement awaits.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync + ?Sized> MaybeSync for T {}
#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSync for T {}

/// One websocket frame, transport-agnostic. Hosts bridge these to their
/// native socket type; channels and the engine never see axum or a
/// platform socket.
#[derive(Debug)]
pub enum WsFrame {
    Text(String),
    Binary(Bytes),
    Close(Option<u16>),
}

/// A connected websocket, either direction. `recv` returning `None`
/// means the peer closed cleanly. `recv` must be cancellation-safe: dropping
/// its future must not consume an in-flight or resolved frame, and the next
/// call must still observe that frame.
pub trait WsDuplex: MaybeSend {
    fn send<'a>(&'a mut self, frame: WsFrame) -> crate::BoxFuture<'a, Result<(), TransportError>>;
    fn recv<'a>(&'a mut self) -> crate::BoxFuture<'a, Result<Option<WsFrame>, TransportError>>;
}
