use std::sync::OnceLock;
use std::time::Duration;

use gproxy_core::CacheBackend;
use gproxy_core::channel_api::BoxFuture;
use redis::Script;
use redis::aio::ConnectionManager;

use super::{error, ttl_millis};

type Error = gproxy_core::error::StoreError;

const INCR_SCRIPT: &str = "local e=redis.call('EXISTS',KEYS[1]); local v=redis.call('INCRBY',KEYS[1],ARGV[1]); if e==0 and tonumber(ARGV[2])>0 then redis.call('PEXPIRE',KEYS[1],ARGV[2]); end; return v";
const COMPARE_INCR_SCRIPT: &str = "if redis.call('GET',KEYS[2])~=ARGV[2] then return false end; local v=redis.call('INCRBY',KEYS[1],ARGV[1]); if v==0 then redis.call('PEXPIRE',KEYS[1],3600000) else redis.call('PERSIST',KEYS[1]) end; redis.call('SET',KEYS[2],ARGV[3]); return v";
const CAS_SCRIPT: &str = "local c=redis.call('GET',KEYS[1]); if (ARGV[1]=='0' and c) or (ARGV[1]=='1' and c~=ARGV[2]) then return 0 end; if ARGV[3]=='1' then if tonumber(ARGV[5])>0 then redis.call('SET',KEYS[1],ARGV[4],'PX',ARGV[5]) else redis.call('SET',KEYS[1],ARGV[4]) end else redis.call('DEL',KEYS[1]) end; return 1";

fn incr_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(INCR_SCRIPT))
}

fn compare_incr_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(COMPARE_INCR_SCRIPT))
}

fn cas_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(CAS_SCRIPT))
}

fn reserve_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(super::spend::RESERVE_SCRIPT))
}

fn reserve_and_set_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(super::spend::RESERVE_AND_SET_SCRIPT))
}

fn raise_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(super::spend::RAISE_SCRIPT))
}

#[derive(Clone)]
pub struct RedisCache {
    connection: ConnectionManager,
}

impl RedisCache {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let client = redis::Client::open(url).map_err(|_| error("Redis", "configuration"))?;
        let connection = ConnectionManager::new(client)
            .await
            .map_err(|_| error("Redis", "connection"))?;
        Ok(Self { connection })
    }

    async fn command<T: redis::FromRedisValue>(
        &self,
        command: &mut redis::Cmd,
        operation: &'static str,
    ) -> Result<T, Error> {
        command
            .query_async(&mut self.connection.clone())
            .await
            .map_err(|_| error("Redis", operation))
    }
}

impl CacheBackend for RedisCache {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, Error>> {
        Box::pin(async move { self.command(redis::cmd("GET").arg(key), "get").await })
    }

    fn set<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let mut command = redis::cmd("SET");
            command.arg(key).arg(value);
            let ttl = ttl_millis(ttl);
            if ttl > 0 {
                command.arg("PX").arg(ttl);
            }
            self.command(&mut command, "set").await
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.command::<u64>(redis::cmd("DEL").arg(key), "delete")
                .await?;
            Ok(())
        })
    }

    fn incr<'a>(
        &'a self,
        key: &'a str,
        by: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<i64, Error>> {
        Box::pin(async move {
            incr_script()
                .key(key)
                .arg(by)
                .arg(ttl_millis(ttl))
                .invoke_async(&mut self.connection.clone())
                .await
                .map_err(|_| error("Redis", "increment"))
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
            compare_incr_script()
                .key(counter_key)
                .key(state_key)
                .arg(by)
                .arg(expected)
                .arg(state)
                .invoke_async(&mut self.connection.clone())
                .await
                .map_err(|_| error("Redis", "compare increment"))
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
            let expected_present = u8::from(expected.is_some());
            let value_present = u8::from(value.is_some());
            let result: i64 = cas_script()
                .key(key)
                .arg(expected_present)
                .arg(expected.unwrap_or_default())
                .arg(value_present)
                .arg(value.unwrap_or_default())
                .arg(ttl_millis(ttl))
                .invoke_async(&mut self.connection.clone())
                .await
                .map_err(|_| error("Redis", "compare and swap"))?;
            Ok(result == 1)
        })
    }

    fn seed_counter<'a>(
        &'a self,
        key: &'a str,
        value: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        Box::pin(async move {
            let mut command = redis::cmd("SET");
            command.arg(key).arg(value).arg("NX");
            if ttl_millis(ttl) > 0 {
                command.arg("PX").arg(ttl_millis(ttl));
            }
            let result: Option<String> = self.command(&mut command, "seed counter").await?;
            Ok(result.is_some())
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
            let code: i64 = reserve_script()
                .key(used_key)
                .key(pending_key)
                .arg(estimate)
                .arg(limit)
                .arg(ttl_millis(pending_ttl))
                .invoke_async(&mut self.connection.clone())
                .await
                .map_err(|_| error("Redis", "reserve spend"))?;
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
        state_key: &'a str,
        expected_state: Vec<u8>,
        state: Vec<u8>,
    ) -> BoxFuture<'a, Result<Option<gproxy_core::SpendReserve>, Error>> {
        Box::pin(async move {
            let code: i64 = reserve_and_set_script()
                .key(used_key)
                .key(pending_key)
                .key(state_key)
                .arg(estimate)
                .arg(limit)
                .arg(expected_state)
                .arg(state)
                .invoke_async(&mut self.connection.clone())
                .await
                .map_err(|_| error("Redis", "reserve spend and set"))?;
            match code {
                -2 => Ok(None),
                -1 => Ok(Some(gproxy_core::SpendReserve::MissingUsed)),
                0 => Ok(Some(gproxy_core::SpendReserve::Denied)),
                1 => Ok(Some(gproxy_core::SpendReserve::Allowed)),
                _ => Err(error("Redis", "reserve spend and set")),
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
            let _: i64 = raise_script()
                .key(key)
                .arg(floor)
                .arg(ttl_millis(ttl))
                .invoke_async(&mut self.connection.clone())
                .await
                .map_err(|_| error("Redis", "raise counter"))?;
            Ok(())
        })
    }
}
