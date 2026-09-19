use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use gproxy_core::{ByteStream, StreamCancellation};
use js_sys::{Object, Promise, Reflect, Uint8Array};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;
use web_sys::{ReadableStream, ReadableStreamDefaultController};

use crate::stream_state::StreamState;

pub(crate) struct StreamBody {
    readable: ReadableStream,
    state: Rc<StreamState>,
}

impl StreamBody {
    pub(crate) async fn new(
        stream: ByteStream,
        cancellation: Option<StreamCancellation>,
    ) -> Result<Self, JsValue> {
        let state = Rc::new(StreamState::new(stream, cancellation));
        let source = Object::new();

        let pull_state = state.clone();
        let pull = Closure::<dyn FnMut(ReadableStreamDefaultController) -> Promise>::new(
            move |controller| {
                future_to_promise(AssertUnwindSafe(pull_once(pull_state.clone(), controller)))
            },
        )
        .into_js_value();
        let cancel_state = state.clone();
        let cancel = Closure::<dyn FnMut(JsValue) -> Promise>::new(move |_| {
            let state = cancel_state.clone();
            future_to_promise(AssertUnwindSafe(async move {
                state.cancel().await;
                Ok(JsValue::UNDEFINED)
            }))
        })
        .into_js_value();

        let readable = Reflect::set(&source, &JsValue::from_str("pull"), &pull)
            .and_then(|_| Reflect::set(&source, &JsValue::from_str("cancel"), &cancel))
            .and_then(|_| ReadableStream::new_with_underlying_source(&source));
        match readable {
            Ok(readable) => Ok(Self { readable, state }),
            Err(error) => {
                state.cancel().await;
                Err(error)
            }
        }
    }

    pub(crate) fn readable(&self) -> ReadableStream {
        self.readable.clone()
    }

    pub(crate) async fn cancel(&self) {
        self.state.cancel().await;
    }
}

async fn pull_once(
    state: Rc<StreamState>,
    controller: ReadableStreamDefaultController,
) -> Result<JsValue, JsValue> {
    let next = state.next().await;
    if state.is_cancelled() {
        return Ok(JsValue::UNDEFINED);
    }
    match next {
        Some(Ok(bytes)) => {
            let Ok(length) = u32::try_from(bytes.len()) else {
                let error = JsValue::from_str("response stream chunk exceeds Uint8Array capacity");
                controller.error_with_e(&error);
                state.cancel().await;
                return Ok(JsValue::UNDEFINED);
            };
            let chunk = Uint8Array::new_with_length(length);
            chunk.copy_from(&bytes);
            if let Err(error) = controller.enqueue_with_chunk(&chunk.into()) {
                state.cancel().await;
                return Err(error);
            }
        }
        Some(Err(error)) => {
            controller.error_with_e(&JsValue::from_str(&error.to_string()));
            state.cancel().await;
        }
        None => controller.close()?,
    }
    Ok(JsValue::UNDEFINED)
}

pub(crate) async fn cancel_stream(stream: ByteStream, cancellation: Option<StreamCancellation>) {
    StreamState::new(stream, cancellation).cancel().await;
}
