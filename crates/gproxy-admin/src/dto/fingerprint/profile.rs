use std::collections::BTreeMap;

use gproxy_channel_api::{Alpn, ClientFingerprint, Http2Setting, PseudoHeader, TlsVersion};

use super::{
    AlpnDto, FingerprintHeadersDto, Http2ProfileDto, PseudoHeaderDto, TlsFingerprintDto,
    TlsProfileDto, TlsVersionDto,
};
use crate::dto::TlsPresetDto;

/// Project a channel's defaults into the editable fingerprint schema. Omit
/// empty defaults and values the schema cannot represent without losing data.
pub fn client_fingerprint_dto(value: ClientFingerprint) -> Option<TlsPresetDto> {
    let profile = value.profile;
    if profile.preset.is_some() || (value.headers.is_empty() && !profile.is_usable()) {
        return None;
    }
    // The configuration parser rejects empty TLS strings and drops empty
    // extension/HTTP2 profiles, so these cannot make a lossless round trip.
    if [
        profile.cipher_list.as_deref(),
        profile.curves_list.as_deref(),
        profile.sigalgs_list.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(str::is_empty)
        || profile
            .extension_permutation
            .as_ref()
            .is_some_and(|values| values.is_empty())
        || profile
            .http2
            .as_ref()
            .is_some_and(|value| value == &Default::default())
    {
        return None;
    }
    let mut headers = BTreeMap::new();
    for (name, value) in &value.headers {
        if headers
            .insert(name.to_string(), value.to_str().ok()?.to_owned())
            .is_some()
        {
            return None;
        }
    }
    Some(TlsPresetDto {
        id: value.id.into(),
        label: value.label.into(),
        fingerprint: TlsFingerprintDto {
            headers: Some(FingerprintHeadersDto::Values(headers)),
            tls: Some(TlsProfileDto {
                alpn_protocols: profile.alpn.as_ref().map(|values| {
                    values
                        .iter()
                        .map(|value| match value {
                            Alpn::Http1 => AlpnDto::Http1,
                            Alpn::Http2 => AlpnDto::Http2,
                            Alpn::Http3 => AlpnDto::Http3,
                        })
                        .collect()
                }),
                grease_enabled: profile.grease,
                min_tls_version: profile.min_tls_version.map(tls_version),
                max_tls_version: profile.max_tls_version.map(tls_version),
                cipher_list: profile.cipher_list.as_deref().map(str::to_owned),
                curves_list: profile.curves_list.as_deref().map(str::to_owned),
                sigalgs_list: profile.sigalgs_list.as_deref().map(str::to_owned),
                preserve_tls13_cipher_list: profile.preserve_tls13_cipher_list,
                extension_permutation: profile.extension_permutation.as_deref().map(<[_]>::to_vec),
            }),
            http2: profile.http2.as_ref().map(|http2| Http2ProfileDto {
                enable_push: http2.enable_push,
                initial_window_size: http2.initial_window_size,
                initial_connection_window_size: http2.initial_connection_window_size,
                max_frame_size: http2.max_frame_size,
                max_header_list_size: http2.max_header_list_size,
                header_table_size: http2.header_table_size,
                max_concurrent_streams: http2.max_concurrent_streams,
                headers_pseudo_order: http2.pseudo_header_order.as_ref().map(|values| {
                    values
                        .iter()
                        .map(|value| match value {
                            PseudoHeader::Method => PseudoHeaderDto::Method,
                            PseudoHeader::Scheme => PseudoHeaderDto::Scheme,
                            PseudoHeader::Authority => PseudoHeaderDto::Authority,
                            PseudoHeader::Path => PseudoHeaderDto::Path,
                        })
                        .collect()
                }),
                settings_order: http2.settings_order.as_ref().map(|values| {
                    values
                        .iter()
                        .map(|value| match value {
                            Http2Setting::HeaderTableSize => 1,
                            Http2Setting::EnablePush => 2,
                            Http2Setting::MaxConcurrentStreams => 3,
                            Http2Setting::InitialWindowSize => 4,
                            Http2Setting::MaxFrameSize => 5,
                            Http2Setting::MaxHeaderListSize => 6,
                        })
                        .collect()
                }),
            }),
        },
    })
}

fn tls_version(value: TlsVersion) -> TlsVersionDto {
    match value {
        TlsVersion::Tls10 => TlsVersionDto::Tls10,
        TlsVersion::Tls11 => TlsVersionDto::Tls11,
        TlsVersion::Tls12 => TlsVersionDto::Tls12,
        TlsVersion::Tls13 => TlsVersionDto::Tls13,
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::LazyLock;

    use gproxy_channel_api::{ClientProfile, ClientProfilePreset, Http2Profile};
    use http::HeaderMap;

    use super::*;

    static PROFILE: LazyLock<ClientProfile> = LazyLock::new(ClientProfile::default);

    fn fingerprint(headers: HeaderMap) -> ClientFingerprint {
        ClientFingerprint {
            id: "test",
            label: "Test",
            headers,
            profile: &PROFILE,
        }
    }

    #[test]
    fn duplicate_headers_cannot_be_exported_losslessly() {
        let mut headers = HeaderMap::new();
        headers.append("x-client-feature", "one".parse().unwrap());
        headers.append("x-client-feature", "two".parse().unwrap());
        assert!(client_fingerprint_dto(fingerprint(headers)).is_none());
    }

    #[test]
    fn empty_fingerprint_is_not_offered_as_a_usable_preset() {
        assert!(client_fingerprint_dto(fingerprint(HeaderMap::new())).is_none());
    }

    #[test]
    fn headers_only_fingerprint_remains_exportable() {
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", "test-client/1.0".parse().unwrap());
        let preset = client_fingerprint_dto(fingerprint(headers)).unwrap();
        let value = serde_json::to_value(preset.fingerprint).unwrap();
        assert_eq!(value["headers"]["user-agent"], "test-client/1.0");
    }

    #[test]
    fn non_text_headers_cannot_be_exported_losslessly() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-client-feature",
            http::HeaderValue::from_bytes(&[0xff]).unwrap(),
        );
        assert!(client_fingerprint_dto(fingerprint(headers)).is_none());
    }

    #[test]
    fn non_representable_profiles_are_omitted_even_with_usable_headers() {
        static PROFILES: LazyLock<[ClientProfile; 6]> = LazyLock::new(|| {
            [
                ClientProfile::preset(ClientProfilePreset::Chrome148),
                ClientProfile {
                    cipher_list: Some(Cow::Borrowed("")),
                    ..Default::default()
                },
                ClientProfile {
                    curves_list: Some(Cow::Borrowed("")),
                    ..Default::default()
                },
                ClientProfile {
                    sigalgs_list: Some(Cow::Borrowed("")),
                    ..Default::default()
                },
                ClientProfile {
                    extension_permutation: Some(Cow::Borrowed(&[])),
                    ..Default::default()
                },
                ClientProfile {
                    http2: Some(Http2Profile::default()),
                    ..Default::default()
                },
            ]
        });
        for profile in &*PROFILES {
            let mut headers = HeaderMap::new();
            headers.insert("user-agent", "test-client/1.0".parse().unwrap());
            let value = ClientFingerprint {
                profile,
                ..fingerprint(headers)
            };
            assert!(client_fingerprint_dto(value).is_none(), "{profile:?}");
        }
    }
}
