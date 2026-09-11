// Both reservation operations share exact decimal arithmetic. Redis Lua's
// numbers cannot represent every i64 counter, including quota boundaries.
macro_rules! spend_script {
    ($body:literal) => {
        concat!(
            r#"
local function magnitude_compare(a, b)
  if #a ~= #b then return #a < #b and -1 or 1 end
  if a == b then return 0 end
  return a < b and -1 or 1
end
local function compare(a, b)
  local an, bn = a:sub(1, 1) == '-', b:sub(1, 1) == '-'
  if an ~= bn then return an and -1 or 1 end
  if an then return -magnitude_compare(a:sub(2), b:sub(2)) end
  return magnitude_compare(a, b)
end
local function normalize(value)
  if not value:match('^%-?%d+$') then error('cache counter is not an i64') end
  local negative = value:sub(1, 1) == '-'
  local digits = (negative and value:sub(2) or value):gsub('^0+', '')
  if digits == '' then return '0' end
  return (negative and '-' or '') .. digits
end
local function add(a, b)
  local an, bn = a:sub(1, 1) == '-', b:sub(1, 1) == '-'
  a, b = an and a:sub(2) or a, bn and b:sub(2) or b
  local subtract = an ~= bn
  if subtract and magnitude_compare(a, b) < 0 then a, b, an = b, a, bn end
  local i, j, carry, result = #a, #b, 0, ''
  while i > 0 or j > 0 do
    local x, y = i > 0 and a:byte(i) - 48 or 0, j > 0 and b:byte(j) - 48 or 0
    local digit = x + (subtract and -y or y) + carry
    if subtract then
      carry = digit < 0 and -1 or 0
      if digit < 0 then digit = digit + 10 end
    else
      carry = math.floor(digit / 10)
      digit = digit % 10
    end
    result = string.char(48 + digit) .. result
    i, j = i - 1, j - 1
  end
  if carry > 0 then result = tostring(carry) .. result end
  return normalize((an and '-' or '') .. result)
end
local minimum, maximum = '-9223372036854775808', '9223372036854775807'
local function integer(value)
  value = normalize(value)
  if compare(value, minimum) < 0 or compare(value, maximum) > 0 then
    error('cache counter overflow')
  end
  return value
end
local function saturate(value)
  if compare(value, minimum) < 0 then return minimum end
  if compare(value, maximum) > 0 then return maximum end
  return value
end
local function positive(value)
  return value:sub(1, 1) == '-' and '0' or value
end

local function reserve(used, pending, estimate, limit)
  used, pending, limit = integer(used), integer(pending), integer(limit)
  local next_pending = integer(add(pending, integer(estimate)))
  if compare(saturate(add(used, positive(pending))), limit) >= 0 or
     compare(saturate(add(used, positive(next_pending))), limit) > 0 then
    return false
  end
  return next_pending
end
"#,
            $body
        )
    };
}

/// KEYS[1]=used KEYS[2]=pending ARGV[1]=estimate ARGV[2]=limit ARGV[3]=ttl_ms
/// Returns -1 missing used, 0 denied, 1 allowed.
pub(crate) const RESERVE_SCRIPT: &str = spend_script!(
    r#"
local used = redis.call('GET', KEYS[1])
if not used then return -1 end
local next_pending = reserve(used, redis.call('GET', KEYS[2]) or '0', ARGV[1], ARGV[2])
if not next_pending then return 0 end
redis.call('SET', KEYS[2], next_pending)
if tonumber(ARGV[3]) > 0 then
  redis.call('PEXPIRE', KEYS[2], ARGV[3])
end
return 1
"#
);

/// KEYS: used, pending, state. ARGV: estimate, limit, expected state, new state.
/// Returns -2 state changed, -1 missing used, 0 denied, 1 allowed.
pub(crate) const RESERVE_AND_SET_SCRIPT: &str = spend_script!(
    r#"
local current = redis.call('GET', KEYS[3])
if current == ARGV[4] then return 1 end
if current ~= ARGV[3] then return -2 end
local used = redis.call('GET', KEYS[1])
if not used then return -1 end
local next_pending = reserve(used, redis.call('GET', KEYS[2]) or '0', ARGV[1], ARGV[2])
if not next_pending then return 0 end
redis.call('SET', KEYS[2], next_pending)
redis.call('SET', KEYS[3], ARGV[4])
return 1
"#
);

/// KEYS[1]=used ARGV[1]=durable floor ARGV[2]=ttl_ms.
/// Compare decimal strings so large integer counters do not lose precision in Lua.
pub(crate) const RAISE_SCRIPT: &str = r#"
local current = redis.call('GET', KEYS[1])
local floor = ARGV[1]
if not current or string.sub(current, 1, 1) == '-' or
   string.len(current) < string.len(floor) or
   (string.len(current) == string.len(floor) and current < floor) then
  redis.call('SET', KEYS[1], floor)
end
if tonumber(ARGV[2]) > 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
  redis.call('PERSIST', KEYS[1])
end
return 1
"#;
