import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { act, cleanup, render } from "@testing-library/react"
import { afterEach, expect, it, vi } from "vitest"
import { ProvidersPage } from "./providers"
import { navigateAdminPath } from "@/lib/admin-route"

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key, i18n: { language: "en" } }) }))

vi.mock("@/components/providers/providers-view", () => ({ ProvidersView: () => null }))

afterEach(() => { cleanup(); vi.useRealTimers(); vi.unstubAllGlobals() })

it("scopes provider polling, stops it on other tabs, and aborts obsolete reads", async () => {
  vi.useFakeTimers()
  const reads: Array<{ body: Record<string, unknown>; signal: AbortSignal; view: string }> = []
  vi.stubGlobal("fetch", vi.fn((url: string, init: RequestInit) => {
    if (url === "/admin/api/credential-cycles/query") {
      reads.push({ body: JSON.parse(init.body as string), signal: init.signal!, view: new Headers(init.headers).get("x-gproxy-console-view")! })
      // The first provider's outstanding read must be cancelled on navigation.
      if (reads.length === 1) return new Promise((_resolve, reject) => {
        init.signal!.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")))
      })
    }
    return Promise.resolve(new Response("[]", { status: 200 }))
  }))
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  window.history.replaceState(null, "", "/admin/providers")
  const mounted = render(<QueryClientProvider client={client}><ProvidersPage /></QueryClientProvider>)
  await act(() => vi.advanceTimersByTimeAsync(60_000))
  expect(reads).toHaveLength(0)
  await act(async () => navigateAdminPath("/admin/providers/1/credentials"))
  expect(reads).toHaveLength(1)
  await act(async () => navigateAdminPath("/admin/providers/2/credentials"))
  expect(reads[0].signal.aborted).toBe(true)
  expect(reads).toHaveLength(2)
  await act(() => vi.advanceTimersByTimeAsync(30_000))
  expect(reads).toHaveLength(3)
  for (const [index, read] of reads.entries()) {
    expect(read.body).toMatchObject({ provider_id: index === 0 ? 1 : 2, current_only: true, include_estimate: false, include_history: false })
    expect(Number(read.body.to) - Number(read.body.from)).toBe(604_800)
    expect(read.view).toBe("providers")
  }
  await act(async () => navigateAdminPath("/admin/providers/2/models"))
  await act(() => vi.advanceTimersByTimeAsync(60_000))
  expect(reads).toHaveLength(3)
  mounted.unmount()
  client.clear()
})
