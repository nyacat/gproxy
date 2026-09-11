import "@testing-library/jest-dom/vitest"
import { cleanup } from "@testing-library/react"
import { afterEach } from "vitest"

class TestResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}

Object.defineProperty(globalThis, "ResizeObserver", { value: TestResizeObserver, writable: true })
Object.defineProperty(window, "matchMedia", { writable: true, value: (query: string) => ({
  media: query, matches: false, onchange: null,
  addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {}, dispatchEvent: () => true,
}) })
Object.defineProperty(Element.prototype, "scrollIntoView", { value() {}, writable: true })

afterEach(cleanup)
