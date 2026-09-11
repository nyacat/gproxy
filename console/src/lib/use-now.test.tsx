import { act, renderHook } from "@testing-library/react"
import { afterEach, expect, it, vi } from "vitest"
import { useNow } from "./use-now"

afterEach(() => vi.useRealTimers())

it("keeps the clock stable during unrelated renders and advances on ticks or focus", () => {
  vi.useFakeTimers()
  vi.setSystemTime(1_000_000)
  const { result, rerender, unmount } = renderHook(useNow)
  expect(result.current).toBe(1000)
  vi.setSystemTime(1_020_000)
  rerender()
  expect(result.current).toBe(1000)
  act(() => window.dispatchEvent(new Event("focus")))
  expect(result.current).toBe(1020)
  act(() => vi.advanceTimersByTime(60_000))
  expect(result.current).toBe(1080)
  unmount()
  expect(vi.getTimerCount()).toBe(0)
})
