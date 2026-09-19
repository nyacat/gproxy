#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(target_arch = "wasm32")]
mod wasm;

use std::time::Duration;

use base64::Engine as _;
use gproxy_core::CacheBackend;
use gproxy_core::channel_api::BoxFuture;
use serde_json::{Value, json};

use super::{error, ttl_millis};

type Error = gproxy_core::error::StoreError;

pub struct UpstashCache {
    url: String,
    token: String,
    sender: Sender,
}

#[cfg(not(target_arch = "wasm32"))]
type Sender = native::NativeSender;
#[cfg(target_arch = "wasm32")]
type Sender = wasm::WasmSender;

impl UpstashCache {
    pub fn new(url: String, token: String) -> Self {
        Self {
            url,
            token,
            sender: Sender::new(),
        }
    }

    async fn command(
        &self,
        arguments: Vec<Value>,
        operation: &'static str,
    ) -> Result<Value, Error> {
        let body = serde_json::to_vec(&arguments).map_err(|_| error("Upstash", operation))?;
        let bytes = self.sender.post(&self.url, &self.token, body).await?;
        let response: Value =
            serde_json::from_slice(&bytes).map_err(|_| error("Upstash", operation))?;
        if response.get("error").is_some() {
            return Err(error("Upstash", operation));
        }
        response
            .get("result")
            .cloned()
            .ok_or_else(|| error("Upstash", operation))
    }

    async fn eval(
        &self,
        script: &str,
        keys: &[&str],
        arguments: Vec<Value>,
        operation: &'static str,
    ) -> Result<Value, Error> {
        let mut command = vec![json!("EVAL"), json!(script), json!(keys.len())];
        command.extend(keys.iter().map(|key| json!(key)));
        command.extend(arguments);
        self.command(command, operation).await
    }
}

impl CacheBackend for UpstashCache {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, Error>> {
        Box::pin(async move {
            let result = self.command(vec![json!("GET"), json!(key)], "get").await?;
            if result.is_null() {
                return Ok(None);
            }
            let encoded = result.as_str().ok_or_else(|| error("Upstash", "get"))?;
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map(Some)
                .map_err(|_| error("Upstash", "get"))
        })
    }

    fn set<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let mut command = vec![
                json!("SET"),
                json!(key),
                json!(base64::engine::general_purpose::STANDARD.encode(value)),
            ];
            let ttl = ttl_millis(ttl);
            if ttl > 0 {
                command.extend([json!("PX"), json!(ttl)]);
            }
            self.command(command, "set").await.map(|_| ())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.command(vec![json!("DEL"), json!(key)], "delete")
                .await
                .map(|_| ())
        })
    }

    fn incr<'a>(
        &'a self,
        key: &'a str,
        by: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<i64, Error>> {
        Box::pin(async move {
            self.eval(
                super::spend::INCR_SCRIPT,
                &[key],
                vec![json!(by.to_string()), json!(ttl_millis(ttl))],
                "increment",
            )
            .await?
            .as_str()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| error("Upstash", "increment"))
        })
    }

    fn compare_incr_and_set<'a>(
        &'a self,
        counter_key: &'a str,
        by: i64,
        state_key: &'a str,
        expected: Vec<u8>,
        state: Vec<u8>,
    ) -> BoxFuture<'a, Result<Option<i64>, Error>> {
        Box::pin(async move {
            let encode = |value| base64::engine::general_purpose::STANDARD.encode(value);
            let result = self
                .eval(
                    super::spend::COMPARE_INCR_SCRIPT,
                    &[counter_key, state_key],
                    vec![
                        json!(by.to_string()),
                        json!(encode(expected)),
                        json!(encode(state)),
                    ],
                    "compare increment",
                )
                .await?;
            if result.is_null() {
                Ok(None)
            } else {
                result
                    .as_str()
                    .and_then(|value| value.parse().ok())
                    .map(Some)
                    .ok_or_else(|| error("Upstash", "compare increment"))
            }
        })
    }

    fn compare_and_swap<'a>(
        &'a self,
        key: &'a str,
        expected: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        Box::pin(async move {
            let script = "local c=redis.call('GET',KEYS[1]); if (ARGV[1]=='0' and c) or (ARGV[1]=='1' and c~=ARGV[2]) then return 0 end; if ARGV[3]=='1' then if tonumber(ARGV[5])>0 then redis.call('SET',KEYS[1],ARGV[4],'PX',ARGV[5]) else redis.call('SET',KEYS[1],ARGV[4]) end else redis.call('DEL',KEYS[1]) end; return 1";
            let encode = |value| base64::engine::general_purpose::STANDARD.encode(value);
            let arguments = vec![
                json!(u8::from(expected.is_some())),
                json!(expected.map(encode).unwrap_or_default()),
                json!(u8::from(value.is_some())),
                json!(value.map(encode).unwrap_or_default()),
                json!(ttl_millis(ttl)),
            ];
            Ok(self
                .eval(script, &[key], arguments, "compare and swap")
                .await?
                .as_i64()
                == Some(1))
        })
    }

    fn seed_counter<'a>(
        &'a self,
        key: &'a str,
        value: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        Box::pin(async move {
            let mut command = vec![
                json!("SET"),
                json!(key),
                json!(value.to_string()),
                json!("NX"),
            ];
            if ttl_millis(ttl) > 0 {
                command.extend([json!("PX"), json!(ttl_millis(ttl))]);
            }
            Ok(!self.command(command, "seed counter").await?.is_null())
        })
    }

    fn reserve_spend<'a>(
        &'a self,
        used_key: &'a str,
        pending_key: &'a str,
        estimate: i64,
        limit: i64,
        pending_ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<gproxy_core::SpendReserve, Error>> {
        Box::pin(async move {
            let code = self
                .eval(
                    super::spend::RESERVE_SCRIPT,
                    &[used_key, pending_key],
                    vec![
                        json!(estimate.to_string()),
                        json!(limit.to_string()),
                        json!(ttl_millis(pending_ttl)),
                    ],
                    "reserve spend",
                )
                .await?
                .as_i64()
                .ok_or_else(|| error("Upstash", "reserve spend"))?;
            Ok(match code {
                -1 => gproxy_core::SpendReserve::MissingUsed,
                0 => gproxy_core::SpendReserve::Denied,
                _ => gproxy_core::SpendReserve::Allowed,
            })
        })
    }

    fn reserve_spend_and_set<'a>(
        &'a self,
        used_key: &'a str,
        pending_key: &'a str,
        estimate: i64,
        limit: i64,
        pending_ttl: Option<Duration>,
        state_key: &'a str,
        expected_state: Vec<u8>,
        state: Vec<u8>,
    ) -> BoxFuture<'a, Result<Option<gproxy_core::SpendReserve>, Error>> {
        Box::pin(async move {
            let encode = |value| base64::engine::general_purpose::STANDARD.encode(value);
            let code = self
                .eval(
                    super::spend::RESERVE_AND_SET_SCRIPT,
                    &[used_key, pending_key, state_key],
                    vec![
                        json!(estimate.to_string()),
                        json!(limit.to_string()),
                        json!(encode(expected_state)),
                        json!(encode(state)),
                        json!(ttl_millis(pending_ttl)),
                    ],
                    "reserve spend and set",
                )
                .await?
                .as_i64()
                .ok_or_else(|| error("Upstash", "reserve spend and set"))?;
            match code {
                -2 => Ok(None),
                -1 => Ok(Some(gproxy_core::SpendReserve::MissingUsed)),
                0 => Ok(Some(gproxy_core::SpendReserve::Denied)),
                1 => Ok(Some(gproxy_core::SpendReserve::Allowed)),
                _ => Err(error("Upstash", "reserve spend and set")),
            }
        })
    }

    fn raise_counter<'a>(
        &'a self,
        key: &'a str,
        floor: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.eval(
                super::spend::RAISE_SCRIPT,
                &[key],
                vec![json!(floor.to_string()), json!(ttl_millis(ttl))],
                "raise counter",
            )
            .await?;
            Ok(())
        })
    }
}
