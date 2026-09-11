import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { act, cleanup, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { afterEach, beforeEach, expect, it, vi } from "vitest"
import "@/i18n"
import type { CredentialQuotaCycleDto } from "@/generated/CredentialQuotaCycleDto"
import { QuotaHistory } from "./quota-history"
import { CycleEstimateDetails } from "./cycle-estimate-details"

vi.mock("./quota-history-chart", () => ({ QuotaHistoryChart: () => <div>chart</div> }))
const cycle: CredentialQuotaCycleDto = {
  id: 1, version: 1, credential_id: 7, window_key: "primary", label: null,
  period_start: 100, period_end: 200, accounting_start_ms: 100000, accounting_end_ms: 200000,
  boundary_source: "upstream", boundary_confidence: "exact", status: "closed", close_reason: "boundary_crossed",
  last_observed_at: 180, upstream_used: null, upstream_limit: null, used_percent: "50", coverage: "partial_lower_bound",
  metrics: {}, models: [], unit: null, local_boundary: false, estimate: null, observations: [],
}
let client: QueryClient
let requests: Array<{ body: Record<string, unknown>; signal: AbortSignal; respond: () => void }>
beforeEach(() => {
  client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: 15000 } } })
  requests = []
  vi.stubGlobal("fetch", vi.fn((_path: string, init: RequestInit) => new Promise<Response>((resolve, reject) => {
    const signal = init.signal!
    signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true })
    requests.push({ body: JSON.parse(init.body as string), signal, respond: () => resolve(new Response(JSON.stringify([cycle]), { status: 200 })) })
  })))
})
afterEach(() => { cleanup(); client.clear(); vi.unstubAllGlobals() })

it("loads only visible rounds when switching from percent to estimates and reuses token/cost reads", async () => {
  const user = userEvent.setup()
  const { unmount } = render(<QueryClientProvider client={client}><QuotaHistory cycles={[cycle, { ...cycle, id: 2 }]} providers={[]} credentials={[]} loading={false} error={false} range={{ from: 100, to: 200 }} /></QueryClientProvider>)
  await waitFor(() => expect(requests).toHaveLength(1))
  expect(requests[0].body).toMatchObject({ include_history: true, include_estimate: false, cycle_ids: [1, 2] })
  await user.click(screen.getByRole("button", { name: "Rounds" }))
  await user.click(screen.getByRole("checkbox", { name: /#2$/ }))
  await user.keyboard("{Escape}")
  const firstEstimate = requests.length
  await user.click(screen.getByRole("radio", { name: "Calculated remaining tokens" }))
  await waitFor(() => expect(requests).toHaveLength(firstEstimate + 1))
  expect(requests[firstEstimate].body).toMatchObject({ cycle_ids: [1], include_history: true, include_estimate: true, from: 100, to: 200 })
  await act(async () => requests[firstEstimate].respond())
  await user.click(screen.getByRole("radio", { name: "Calculated remaining USD" }))
  expect(requests).toHaveLength(firstEstimate + 1)
  await user.click(screen.getByRole("button", { name: "Rounds" }))
  await user.click(screen.getByRole("checkbox", { name: /#2$/ }))
  await user.keyboard("{Escape}")
  await waitFor(() => expect(requests).toHaveLength(firstEstimate + 2))
  expect(requests[firstEstimate + 1].body.cycle_ids).toEqual([1, 2])
  unmount()
  expect(requests[firstEstimate + 1].signal.aborted).toBe(true)
})

it("reads an exact cycle only while its estimate details are open", async () => {
  const user = userEvent.setup()
  render(<QueryClientProvider client={client}><CycleEstimateDetails cycle={cycle} /></QueryClientProvider>)
  expect(requests).toHaveLength(0)
  const trigger = screen.getByRole("button")
  await user.click(trigger)
  await waitFor(() => expect(requests).toHaveLength(1))
  expect(requests[0].body).toMatchObject({ cycle_ids: [1], credential_id: 7, include_estimate: true })
  expect(requests[0].body.include_history).not.toBe(true)
  await user.click(trigger)
  expect(requests[0].signal.aborted).toBe(true)
})

it("paginates cycle rendering and estimates, and cancels estimates for the previous page", async () => {
  const user = userEvent.setup()
  const cycles = Array.from({ length: 13 }, (_, i) => ({ ...cycle, id: i + 1 }))
  render(<QueryClientProvider client={client}><QuotaHistory cycles={cycles} providers={[]} credentials={[]} loading={false} error={false} range={{ from: 100, to: 200 }} /></QueryClientProvider>)
  expect(screen.getAllByRole("button", { name: "View estimate details" })).toHaveLength(10)
  await waitFor(() => expect(requests).toHaveLength(1))
  const firstEstimate = requests.length
  await user.click(screen.getByRole("radio", { name: "Calculated remaining tokens" }))
  await waitFor(() => expect(requests).toHaveLength(firstEstimate + 1))
  expect(requests[0].body).toMatchObject({ include_estimate: false, cycle_ids: [4, 5, 6, 7, 8, 9, 10, 11, 12, 13] })
  expect(requests[firstEstimate].body.include_estimate).toBe(true)
  await user.click(screen.getByRole("button", { name: "Next" }))
  await waitFor(() => expect(requests).toHaveLength(firstEstimate + 2))
  expect(requests[firstEstimate].signal.aborted).toBe(true)
  expect(requests[firstEstimate + 1].body.cycle_ids).toEqual([1, 2, 3])
  expect(screen.getAllByRole("button", { name: "View estimate details" })).toHaveLength(3)
})
