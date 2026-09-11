// Formatting is synchronous. Keep that work bounded even for multi-megabyte
// request bodies; large bodies remain verbatim and are browsed in segments.
export const LOG_FORMAT_LIMIT = 64 * 1024
export const LOG_SEGMENT_SIZE = 16 * 1024

export function formattedLogContent(value: string) {
  if (value.length > LOG_FORMAT_LIMIT) return value
  // A small but deeply nested JSON value can expand quadratically when indented.
  let depth = 0, quoted = false, escaped = false
  for (const char of value) {
    if (quoted) {
      if (escaped) escaped = false
      else if (char === "\\") escaped = true
      else if (char === '"') quoted = false
    } else if (char === '"') quoted = true
    else if (char === "{" || char === "[") { if (++depth > 64) return value }
    else if (char === "}" || char === "]") depth--
  }
  try {
    return JSON.stringify(JSON.parse(value), null, 2)
  } catch {
    return value
  }
}

export function logSegment(value: string, page: number) {
  const boundary = (offset: number) => {
    const code = value.charCodeAt(offset)
    const previous = value.charCodeAt(offset - 1)
    return code >= 0xdc00 && code <= 0xdfff && previous >= 0xd800 && previous <= 0xdbff ? offset - 1 : offset
  }
  return value.slice(boundary(page * LOG_SEGMENT_SIZE), boundary((page + 1) * LOG_SEGMENT_SIZE))
}
