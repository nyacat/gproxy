use bytes::Bytes;
use http::{Response, StatusCode};

use crate::{AdminError, State, response};

pub(super) fn channels(state: &impl State) -> Result<Response<Bytes>, AdminError> {
    response::json(StatusCode::OK, &state.channel_catalogue())
}

pub(super) fn tls_presets(state: &impl State) -> Result<Response<Bytes>, AdminError> {
    response::json(StatusCode::OK, &state.tls_presets())
}
