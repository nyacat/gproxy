//! Measure the classifier's allocations independently of the input fixture.
//! Keeping this in its own test binary isolates the allocator from other tests.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use gproxy_channel_api::{Channel, StreamCtx, channel::StreamStartState};
use gproxy_protocol::{ContentGenerationKind, Operation, OperationKey, StreamFraming};

struct Allocations;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn added(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: Every operation is delegated to System with the original layout;
// accounting uses atomics and never allocates or changes pointer ownership.
unsafe impl GlobalAlloc for Allocations {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            added(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            added(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe {
            System.dealloc(ptr, layout);
        }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !ptr.is_null() {
            if new_size >= layout.size() {
                added(new_size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        ptr
    }
}

#[global_allocator]
static ALLOCATOR: Allocations = Allocations;

#[test]
fn large_instructions_and_dense_tool_schemas_are_borrowed_without_a_json_tree() {
    let headers = http::HeaderMap::new();
    let body = Bytes::new();
    let probe = || {
        gproxy_channels::CodexChannel
            .stream_start(StreamCtx {
                key: OperationKey::content(
                    Operation::StreamGenerateContent,
                    ContentGenerationKind::OpenAiResponses,
                ),
                framing: StreamFraming::Sse,
                request_body: &body,
                response_headers: &headers,
            })
            .unwrap()
    };
    let failure =
        b"data: {\"type\":\"error\",\"code\":\"server_is_overloaded\",\"message\":\"busy\"}\n\n";
    // Initialize shared diagnostic regexes before measuring per-request work.
    probe().inspect(failure, false).unwrap();
    for metadata_bytes in [1024 * 1024, 32 * 1024 * 1024] {
        let wire = format!(
            "data: {{\"type\":\"response.created\",\"response\":{{\"id\":\"r\",\"object\":\"response\",\"created_at\":1,\"status\":\"in_progress\",\"output\":[],\"instructions\":\"{}\",\"tools\":[{{\"parameters\":{{\"enum\":[{}0]}}}}]}}}}\n\n",
            "\\u0061".repeat(metadata_bytes / 12),
            "0,".repeat(metadata_bytes / 4)
        );
        let mut probe = probe();
        let before = LIVE.load(Ordering::Relaxed);
        PEAK.store(before, Ordering::Relaxed);
        let state = probe.inspect(wire.as_bytes(), false).unwrap();
        let peak = PEAK.load(Ordering::Relaxed).saturating_sub(before);
        assert!(matches!(state, StreamStartState::Pending));
        assert!(
            peak < 128 * 1024,
            "{metadata_bytes} byte metadata allocated {peak} temporary bytes"
        );
        println!("metadata_bytes={metadata_bytes} peak_temporary_bytes={peak}");
    }
    // Exercise the paths that legitimately need scratch: an escaped unknown
    // key and deeply nested ignored metadata, combined with multiline framing.
    for metadata in [
        format!("\"{}\":null", "\\u0061".repeat(100_000)),
        format!(
            "\"instructions\":{}0{}",
            "[".repeat(100_000),
            "]".repeat(100_000)
        ),
    ] {
        let wire = format!(
            "data: {{\"type\":\"response.created\",\n\
            data: \"response\":{{\"output\":[],{metadata}}}}}\n\n"
        );
        let mut probe = probe();
        let allowed = probe.scratch_bytes(wire.as_bytes()) + 128 * 1024;
        let before = LIVE.load(Ordering::Relaxed);
        PEAK.store(before, Ordering::Relaxed);
        let state = probe.inspect(wire.as_bytes(), false).unwrap();
        let peak = PEAK.load(Ordering::Relaxed).saturating_sub(before);
        assert!(matches!(state, StreamStartState::Pending));
        assert!(
            peak <= allowed,
            "parser exceeded reservation: {peak} > {allowed}"
        );
    }
}
