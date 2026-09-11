import { describe, expect, it } from "vitest"
import { formattedLogContent, logSegment, LOG_FORMAT_LIMIT, LOG_SEGMENT_SIZE } from "./log-content"

describe("bounded log content", () => {
  it("formats small JSON and preserves large or deeply nested payloads verbatim", () => {
    expect(formattedLogContent('{"x":1}')).toBe('{\n  "x": 1\n}')
    const large = JSON.stringify({ value: "x".repeat(LOG_FORMAT_LIMIT) })
    const nested = "[".repeat(10_000) + "1" + "]".repeat(10_000)
    expect(formattedLogContent(large)).toBe(large)
    expect(formattedLogContent(nested)).toBe(nested)
    expect(formattedLogContent("data: invalid JSON\n\n")).toBe("data: invalid JSON\n\n")
  })

  it("retains every character across segments, including Unicode at boundaries", () => {
    const value = "x".repeat(LOG_SEGMENT_SIZE - 1) + "😀" + "a".repeat(LOG_SEGMENT_SIZE - 2) + "😀tail"
    const segments = Array.from({ length: Math.ceil(value.length / LOG_SEGMENT_SIZE) }, (_, page) => logSegment(value, page))
    expect(segments.join("")).toBe(value)
    expect(segments.every((segment) => segment.length <= LOG_SEGMENT_SIZE + 1)).toBe(true)
    expect(segments[1].startsWith("😀")).toBe(true)
    expect(logSegment("\udc00x", 0)).toBe("\udc00x")
  })
})
