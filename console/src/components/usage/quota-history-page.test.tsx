import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react"
import { afterEach, beforeEach, expect, it, vi } from "vitest"
import type { CredentialQuotaCycleDto } from "@/generated/CredentialQuotaCycleDto"
import { QuotaHistoryPage } from "./quota-history-page"
import "@/i18n"

vi.mock("./quota-history-charts", () => ({ QuotaHistoryCharts: () => null }))

const cycle: CredentialQuotaCycleDto = {
  id: 101, version: 1, credential_id: 7, window_key: "primary", label: null,
  period_start: 100, period_end: 200, accounting_start_ms: 100000, accounting_end_ms: 200000,
  boundary_source: "upstream", boundary_confidence: "exact", status: "closed", close_reason: "boundary_crossed",
  last_observed_at: 180, upstream_used: null, upstream_limit: null, used_percent: "50", coverage: "partial_lower_bound",
  metrics: {}, models: [], unit: null, local_boundary: false, estimate: null, observations: [],
}
let client: QueryClient
let requests: Array<{ body: Record<string, unknown>; signal: AbortSignal; respond: (body: unknown) => void }>
beforeEach(() => {
  client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  requests = []
  vi.stubGlobal("fetch", vi.fn((_path: string, init: RequestInit) => new Promise<Response>((resolve, reject) => {
    const signal = init.signal!
    signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true })
    requests.push({ body: JSON.parse(init.body as string), signal, respond: (body) => resolve(new Response(JSON.stringify(body), { status: 200 })) })
  })))
})
afterEach(() => { cleanup(); client.clear(); vi.unstubAllGlobals() })

it("fetches older history with the server cursor and returns to previous pages without retaining their bodies", async () => {
  render(<QueryClientProvider client={client}><QuotaHistoryPage range={{ from: 0, to: 200, provider_id: 3, credential_id: 7 }} providers={[]} credentials={[]} loading={false} error={false} /></QueryClientProvider>)
  expect(requests[0].body).toMatchObject({ from: 0, to: 200, provider_id: 3, credential_id: 7, cursor: null, limit: 10 })
  const cursor = { last_observed_at: 180, id: 101 }
  await act(async () => requests[0].respond({ items: [cycle], next_cursor: cursor }))
  await screen.findByText(/#101 · Period starts:/)
  fireEvent.click(screen.getByRole("button", { name: "Next" }))
  await waitFor(() => expect(requests).toHaveLength(2))
  expect(requests[1].body.cursor).toEqual(cursor)
  expect(screen.queryByText(/#101 · Period starts:/)).not.toBeInTheDocument()
  await act(async () => requests[1].respond({ items: [{ ...cycle, id: 1 }], next_cursor: null }))
  await screen.findByText(/#1 · Period starts:/)
  expect(screen.getByRole("button", { name: "Next" })).toBeDisabled()
  fireEvent.click(screen.getByRole("button", { name: "Previous" }))
  await waitFor(() => expect(requests).toHaveLength(3))
  expect(requests[2].body.cursor).toBeNull()
  expect(client.getQueryCache().findAll({ queryKey: ["credential-cycles", "page"] })).toHaveLength(1)
})

it("filters every page by window and cancels the obsolete page when its global scope changes", async () => {
  const range = { from: 0, to: 200, provider_id: 3, credential_id: 7 }
  const view = (providerId: number) => <QueryClientProvider client={client}><QuotaHistoryPage range={{ ...range, provider_id: providerId }} providers={[]} credentials={[]} loading={false} error={false} /></QueryClientProvider>
  const mounted = render(view(3))
  await act(async () => requests[0].respond({ items: [cycle], next_cursor: { last_observed_at: 180, id: 101 } }))
  await screen.findByText(/#101 · Period starts:/)
  fireEvent.click(screen.getByRole("button", { name: "Next" }))
  await waitFor(() => expect(requests).toHaveLength(2))
  fireEvent.change(screen.getByLabelText("Window key"), { target: { value: "weekly" } })
  await waitFor(() => expect(requests).toHaveLength(3))
  expect(requests[1].signal.aborted).toBe(true)
  expect(requests[2].body).toMatchObject({ window_key: "weekly", cursor: null })
  mounted.rerender(view(4))
  await waitFor(() => expect(requests).toHaveLength(4))
  expect(requests[2].signal.aborted).toBe(true)
  expect(requests[3].body).toMatchObject({ provider_id: 4, window_key: "weekly", cursor: null })
  mounted.unmount()
  expect(requests[3].signal.aborted).toBe(true)
})
