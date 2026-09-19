use std::hash::{DefaultHasher, Hash, Hasher};

use gproxy_channel_api::{
    Alpn, ClientProfile, ClientProfilePreset, Http2Setting, PseudoHeader, TlsVersion,
};
use wreq::IntoEmulation;
use wreq::http2::{Http2Options, PseudoId, PseudoOrder, SettingId, SettingsOrder};
use wreq::tls::{AlpnProtocol, ExtensionType, TlsOptions, TlsVersion as WreqTlsVersion};

/// Built emulations, keyed by the profile that produced them.
///
/// A profile is either a channel constant or a provider's configured
/// fingerprint, so the live set is small and changes only on reload, while
/// building one costs a copy of every cipher string, extension list and
/// default header it carries — a browser preset is the better part of a
/// hundred allocations. Editing a provider's fingerprint leaves the previous
/// profile behind with nothing to evict it, so the map is emptied rather than
/// grown past anything a real deployment holds.
#[derive(Default)]
pub(super) struct Emulations {
    built: std::sync::RwLock<std::collections::HashMap<ClientProfile, wreq::Emulation>>,
}

const MAX_CACHED_EMULATIONS: usize = 64;

impl Emulations {
    pub(super) fn get(&self, profile: &ClientProfile) -> wreq::Emulation {
        if let Some(found) = self.built.read().expect("emulation cache").get(profile) {
            return found.clone();
        }
        let built = client_emulation(profile);
        let mut cache = self.built.write().expect("emulation cache");
        if cache.len() >= MAX_CACHED_EMULATIONS {
            cache.clear();
        }
        cache.insert(profile.clone(), built.clone());
        built
    }
}

fn client_emulation(profile: &ClientProfile) -> wreq::Emulation {
    if let Some(preset) = profile.preset {
        // A capture is applied whole or not at all, so the rest of the profile
        // is dropped here. Nothing in tree sets both; say so loudly rather than
        // let a channel ship a fingerprint it does not actually send.
        if profile.has_transport_overrides() {
            tracing::warn!(
                preset = ?preset,
                "client profile preset is applied whole; its transport overrides are ignored"
            );
        }
        return match preset {
            ClientProfilePreset::Chrome148 => wreq_util::Emulation::Chrome148.into_emulation(),
        };
    }
    let mut emulation = wreq::Emulation::builder();
    let mut tls = TlsOptions::builder();
    let mut has_tls = false;
    if let Some(alpn) = &profile.alpn {
        tls = tls.alpn_protocols(alpn.iter().map(map_alpn));
        has_tls = true;
    }
    if let Some(value) = profile.grease {
        tls = tls.grease_enabled(value);
        has_tls = true;
    }
    if let Some(value) = profile.min_tls_version {
        tls = tls.min_tls_version(map_version(value));
        has_tls = true;
    }
    if let Some(value) = profile.max_tls_version {
        tls = tls.max_tls_version(map_version(value));
        has_tls = true;
    }
    // wreq keeps these as `Cow`, and a channel constant is already borrowed:
    // hand the same `Cow` over instead of copying the string out of it.
    if let Some(value) = &profile.cipher_list {
        tls = tls.cipher_list(value.clone());
        has_tls = true;
    }
    if let Some(value) = &profile.curves_list {
        tls = tls.curves_list(value.clone());
        has_tls = true;
    }
    if let Some(value) = &profile.sigalgs_list {
        tls = tls.sigalgs_list(value.clone());
        has_tls = true;
    }
    if let Some(value) = profile.preserve_tls13_cipher_list {
        tls = tls.preserve_tls13_cipher_list(value);
        has_tls = true;
    }
    if let Some(value) = &profile.extension_permutation {
        tls = tls.extension_permutation(std::borrow::Cow::Owned(
            value.iter().copied().map(ExtensionType::from).collect(),
        ));
        has_tls = true;
    }
    if has_tls {
        emulation = emulation.tls_options(tls.build());
    }
    if let Some(http2) = &profile.http2 {
        emulation = emulation.http2_options(http2_options(http2));
    }
    // wreq keys pooled connections by group, not by TLS/HTTP2 options. Hash
    // the complete value so different profiles cannot share an established
    // connection, while borrowed channel defaults and owned overrides can.
    let mut identity = DefaultHasher::new();
    profile.hash(&mut identity);
    emulation.build(wreq::Group::new(identity.finish()))
}

fn http2_options(profile: &gproxy_channel_api::Http2Profile) -> wreq::http2::Http2Options {
    let mut output = Http2Options::builder();
    if let Some(value) = profile.enable_push {
        output = output.enable_push(value);
    }
    if let Some(value) = profile.initial_window_size {
        output = output.initial_window_size(value);
    }
    if let Some(value) = profile.initial_connection_window_size {
        output = output.initial_connection_window_size(value);
    }
    if let Some(value) = profile.max_frame_size {
        output = output.max_frame_size(value);
    }
    if let Some(value) = profile.max_header_list_size {
        output = output.max_header_list_size(value);
    }
    if let Some(value) = profile.header_table_size {
        output = output.header_table_size(value);
    }
    if let Some(value) = profile.max_concurrent_streams {
        output = output.max_concurrent_streams(value);
    }
    if let Some(order) = &profile.pseudo_header_order {
        output = output.headers_pseudo_order(
            PseudoOrder::builder()
                .extend(order.iter().map(map_pseudo))
                .build(),
        );
    }
    if let Some(order) = &profile.settings_order {
        output = output.settings_order(
            SettingsOrder::builder()
                .extend(order.iter().map(map_setting))
                .build(),
        );
    }
    output.build()
}

fn map_alpn(value: &Alpn) -> AlpnProtocol {
    match value {
        Alpn::Http1 => AlpnProtocol::HTTP1,
        Alpn::Http2 => AlpnProtocol::HTTP2,
        Alpn::Http3 => AlpnProtocol::HTTP3,
    }
}

fn map_version(value: TlsVersion) -> WreqTlsVersion {
    match value {
        TlsVersion::Tls10 => WreqTlsVersion::TLS_1_0,
        TlsVersion::Tls11 => WreqTlsVersion::TLS_1_1,
        TlsVersion::Tls12 => WreqTlsVersion::TLS_1_2,
        TlsVersion::Tls13 => WreqTlsVersion::TLS_1_3,
    }
}

fn map_pseudo(value: &PseudoHeader) -> PseudoId {
    match value {
        PseudoHeader::Method => PseudoId::Method,
        PseudoHeader::Scheme => PseudoId::Scheme,
        PseudoHeader::Authority => PseudoId::Authority,
        PseudoHeader::Path => PseudoId::Path,
    }
}

fn map_setting(value: &Http2Setting) -> SettingId {
    match value {
        Http2Setting::HeaderTableSize => SettingId::HeaderTableSize,
        Http2Setting::EnablePush => SettingId::EnablePush,
        Http2Setting::MaxConcurrentStreams => SettingId::MaxConcurrentStreams,
        Http2Setting::InitialWindowSize => SettingId::InitialWindowSize,
        Http2Setting::MaxFrameSize => SettingId::MaxFrameSize,
        Http2Setting::MaxHeaderListSize => SettingId::MaxHeaderListSize,
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;

    #[test]
    fn a_preset_is_applied_whole_and_keeps_none_of_the_fields_beside_it() {
        let mixed = ClientProfile {
            cipher_list: Some(Cow::Borrowed("TLS_AES_128_GCM_SHA256")),
            ..ClientProfile::preset(ClientProfilePreset::Chrome148)
        };
        assert!(mixed.has_transport_overrides());

        let captured = client_emulation(&ClientProfile::preset(ClientProfilePreset::Chrome148));
        let built = client_emulation(&mixed);

        // The capture wins outright. Splicing one field into a browser
        // fingerprint yields a client nobody ships, which is why the override
        // is dropped and logged rather than merged.
        assert_eq!(
            built.tls_options.unwrap().cipher_list,
            captured.tls_options.unwrap().cipher_list
        );
    }

    #[test]
    fn equal_profiles_share_one_build_and_the_cache_stays_bounded() {
        let cache = Emulations::default();
        // Borrowed channel defaults and an owned copy of the same values are
        // the same profile, so they must not occupy two entries — the
        // connection pool already keys them together.
        cache.get(&ClientProfile {
            alpn: Some(Cow::Borrowed(&[Alpn::Http1])),
            ..Default::default()
        });
        cache.get(&ClientProfile {
            alpn: Some(Cow::Owned(vec![Alpn::Http1])),
            ..Default::default()
        });
        assert_eq!(cache.built.read().unwrap().len(), 1);

        for value in 0..MAX_CACHED_EMULATIONS as u32 {
            cache.get(&ClientProfile {
                http2: Some(gproxy_channel_api::Http2Profile {
                    initial_window_size: Some(value),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        assert!(cache.built.read().unwrap().len() <= MAX_CACHED_EMULATIONS);
    }
}
