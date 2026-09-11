# Built-in client fingerprint maintenance

Baseline checked on 2026-09-11:

| Channel | Release baseline | Release source |
| --- | --- | --- |
| Codex | `0.154.0` | [Official changelog](https://developers.openai.com/codex/changelog/) and `@openai/codex` npm metadata |
| Claude Code | `2.1.268` | [Official changelog](https://code.claude.com/docs/en/changelog) and `@anthropic-ai/claude-code` npm metadata |

The CLI versions are compatibility baselines. Codex's fallback User-Agent uses
the CLI format and the previous preset's simulated desktop environment:
`codex_cli_rs/0.154.0 (Debian 13.0.0; x86_64) xterm-256color`.
This is a fixed client profile, not detection of the proxy host's operating
system. Claude Code uses `claude-cli/2.1.268 (external, cli)`. Neither default
includes proxy branding. Caller-provided Codex identity headers remain subject
to the channel's existing forwarding policy.

## One definition per channel

`Channel::client_fingerprint()` supplies the console's preset catalog. Each
channel returns its request metadata defaults and the same `ClientProfile`
reference used during request preparation. The admin DTO conversion exports
all representable TLS and HTTP/2 fields. No credential, account, request ID,
or session ID is included in the catalog.

The preset IDs remain `claude`, `codex`, `gemini`, `antigravity`, `kiro`, and
`copilot`. Gemini, Antigravity, Kiro, and Copilot now export their existing
channel defaults; their release baselines were not independently updated in
this maintenance pass.

Saving a preset stores its JSON values. Existing provider and credential
overrides remain operator-owned snapshots; they are not rewritten on upgrade.
Re-select the preset to refresh a saved snapshot, or remove the override to
use the current channel defaults.

## TLS scope

These profiles configure GPROXY's native wreq/BoringSSL transport. They do not
establish byte-for-byte equivalence with an upstream client's ClientHello or
HTTP header ordering. No current official-client packet captures were used in
this update.

Codex 0.154.0 constructs a reqwest client and can use different TLS backends.
The reference implementation is
[`HttpClientBuilder`](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/http-client/src/client_builder.rs).
GPROXY's Codex profile advertises HTTP/2 followed by HTTP/1.1 so endpoints that
only negotiate HTTP/1.1 can be used too. Claude Code retains its existing
HTTP/1.1 transport baseline.

Explicit TLS 1.3 cipher ordering now sets `preserve_tls13_cipher_list = true`.
Without this option, BoringSSL replaces the TLS 1.3 portion with its default
ordering even when `cipher_list` contains explicit TLS 1.3 suites. Cipher,
curve, and signature algorithm lists otherwise retain their existing channel
baselines.

Native connection pooling groups custom profiles by their complete TLS and
HTTP/2 values. Changing either layer establishes a separate pool entry for
the same endpoint; equal borrowed defaults and owned configuration values
can still reuse a connection. This matters because wreq does not include
per-request transport options in its pool key automatically.

Claude Code's SDK/runtime metadata remains the existing compatibility
baseline (`0.112.1`, `node`, `v26.3.0`). A CLI release number or npm's Node
engine requirement does not establish the embedded runtime of a native
installation. Update those fields only when a matching runtime/source
reference is available.

Edge runtimes own their TLS stack and cannot apply these native transport
settings; explicit transport overrides continue to fail there.

## Verification

Channel request tests check that exported defaults match prepared requests
and preserve supported caller identity metadata. The app fingerprint test
serializes every registered preset through the public DTO and reads it with
the real configuration parser, comparing the complete profiles and headers.
This catches truncated cipher lists, dropped signature algorithms, omitted
HTTP/2 settings, and inconsistent client versions across the catalog and
request defaults.

The admin integration test reads all six presets through `/admin/api/tls-presets`,
saves them as provider overrides, restarts the app, and compares both the
returned provider DTOs and the stored values parsed by the runtime. A local
HTTP keep-alive peer checks that different TLS/HTTP2 values use separate
connections and equal values reuse the original connection.

A local TCP peer also checks that each exported profile can start a TLS
handshake through the native transport. It closes before the certificate and
HTTP exchanges; this detects invalid transport options without contacting a
provider and is not an end-to-end TLS negotiation test.

Embedding hosts expose their registered channels through `State::tls_presets`;
its default implementation returns an empty list. `AppHandle` provides the
built-in projection. Opaque transport-library presets are omitted when the
editable JSON schema cannot represent them faithfully. Duplicate or non-text
header values and empty/unrepresentable profiles are also omitted, rather
than exporting a preset that loses data or cannot be loaded.
