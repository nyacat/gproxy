import type { ComponentProps, ReactNode } from "react"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { UsagePage } from "./usage"
import { LogsPage } from "./logs"
import { OverviewPage } from "./overview"
import type { UsageExplorer } from "@/components/usage/usage-explorer"
import type { LogExplorer } from "@/components/logs/log-explorer"
import type { OverviewDashboard } from "@/components/overview/overview-dashboard"

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key, i18n: { language: "en" } }),
}))
vi.mock("@/components/page-layout", () => ({ PageLayout: ({ children }: { children: ReactNode }) => children }))
vi.mock("@/components/observability-tabs", () => ({ ObservabilityTabs: () => null }))
vi.mock("@/components/usage/quota-history", () => ({ QuotaHistory: () => <div>quota history</div> }))
vi.mock("@/components/usage/usage-explorer", () => ({
  UsageExplorer: ({ draft, onDraft, onApply, view, page, children }: ComponentProps<typeof UsageExplorer>) => <div>
    <input aria-label="model" value={draft.model ?? ""} onChange={(event) => onDraft({ ...draft, model: event.target.value })} />
    <button onClick={() => onDraft({ ...draft, provider_id: 7, credential_id: 9 })}>filter provider</button>
    <button onClick={onApply}>apply</button>
    {view === "records" ? <div>{page.items[0]?.request_id ?? "empty records"}</div> : children}
  </div>,
}))
vi.mock("@/components/logs/log-explorer", () => ({
  LogExplorer: ({ draft, onDraft, onSearch, onSelect, detailError }: ComponentProps<typeof LogExplorer>) => <div>
    <input aria-label="request id" value={draft.request_id ?? ""} onChange={(event) => onDraft({ ...draft, request_id: event.target.value })} />
    <button onClick={onSearch}>search</button>
    <button onClick={() => onSelect("first")}>first detail</button>
    <button onClick={() => onSelect("second")}>second detail</button>
    {detailError ? <div role="alert">detail error</div> : null}
  </div>,
}))
vi.mock("@/components/overview/overview-dashboard", () => ({
  OverviewDashboard: (props: ComponentProps<typeof OverviewDashboard>) => <output data-testid="dashboard">{JSON.stringify(props)}</output>,
}))

type Request = {
  url: URL
  signal: AbortSignal
  body?: Record<string, unknown>
  respond: (body: unknown) => void
}

const clients: Array<QueryClient> = []
let requests: Array<Request>

beforeEach(() => {
  window.localStorage.clear()
  window.history.replaceState(null, "", "/admin")
  requests = []
  vi.stubGlobal("fetch", vi.fn((path: string, init: RequestInit) => new Promise<Response>((resolve, reject) => {
    const signal = init.signal!
    const abort = () => reject(new DOMException("The request was aborted", "AbortError"))
    signal.addEventListener("abort", abort, { once: true })
    requests.push({
      url: new URL(path, "http://localhost"),
      signal,
      body: typeof init.body === "string" ? JSON.parse(init.body) : undefined,
      respond: (body) => {
        signal.removeEventListener("abort", abort)
        resolve(new Response(JSON.stringify(body), { status: 200 }))
      },
    })
  })))
})

afterEach(() => {
  cleanup()
  for (const client of clients.splice(0)) client.clear()
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

function mount(page: ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: 15_000 } } })
  clients.push(client)
  return { ...render(<QueryClientProvider client={client}>{page}</QueryClientProvider>), client }
}

function matching(path: string) {
  return requests.filter((request) => request.url.pathname === `/admin/api/${path}`)
}

function initialBody(request: Request) {
  switch (request.url.pathname) {
    case "/admin/api/usage-records": return { items: [], page: 1, page_size: 10, total: 0 }
    case "/admin/api/usage-summary": return { requests: 0, cost: "0", total_tokens: "0" }
    case "/admin/api/logs": return { items: [], next_cursor: null }
    default: return []
  }
}

async function completeInitial() {
  await act(async () => {
    for (const request of requests) request.respond(initialBody(request))
  })
}

describe("observability request lifetimes", () => {
  it("reissues an explicit search when the filters and pinned range have not changed", async () => {
    const clock = vi.spyOn(Date, "now").mockReturnValue(1_780_000_000_000)
    try {
      const { unmount } = mount(<UsagePage />)
      await completeInitial()
      fireEvent.click(screen.getByText("apply"))
      await waitFor(() => expect(matching("usage-records")).toHaveLength(2))
      expect(matching("usage-summary")).toHaveLength(2)
      unmount()
      mount(<LogsPage />)
      await completeInitial()
      fireEvent.click(screen.getByText("search"))
      await waitFor(() => expect(matching("logs")).toHaveLength(2))
    } finally { clock.mockRestore() }
  })
  it("lets a new filter cancel the initial records request before any results arrive", async () => {
    mount(<UsagePage />)
    const initial = matching("usage-records")[0]
    expect(initial.url.searchParams.get("include_total")).toBe("false")
    fireEvent.change(screen.getByLabelText("model"), { target: { value: "narrowed" } })
    fireEvent.click(screen.getByText("apply"))
    await waitFor(() => expect(initial.signal.aborted).toBe(true))
    expect(matching("usage-records")[1].url.searchParams.get("model")).toBe("narrowed")
  })
  it("cancels obsolete usage filters and inactive views, and sends provider filters to the API", async () => {
    const { unmount } = mount(<UsagePage />)
    await completeInitial()
    await screen.findByText("empty records")

    fireEvent.change(screen.getByLabelText("model"), { target: { value: "first" } })
    fireEvent.click(screen.getByText("apply"))
    await waitFor(() => expect(matching("usage-summary")).toHaveLength(2))
    const firstRecords = matching("usage-records")[1]
    const firstSummary = matching("usage-summary")[1]

    fireEvent.change(screen.getByLabelText("model"), { target: { value: "second" } })
    fireEvent.click(screen.getByText("filter provider"))
    fireEvent.click(screen.getByText("apply"))
    await waitFor(() => expect(firstSummary.signal.aborted).toBe(true))
    expect(firstRecords.signal.aborted).toBe(true)
    const secondRecords = matching("usage-records")[2]
    const secondSummary = matching("usage-summary")[2]

    fireEvent.click(screen.getByRole("radio", { name: "usage.view.quotas" }))
    await waitFor(() => expect(matching("credential-cycles/query")).toHaveLength(1))
    expect(secondRecords.signal.aborted).toBe(true)
    expect(secondSummary.signal.aborted).toBe(true)
    const history = matching("credential-cycles/query")[0]
    expect(history.body?.provider_id).toBe(7)
    expect(history.body?.credential_id).toBe(9)
    expect(history.body?.include_history).toBe(false)
    expect(history.body?.include_estimate).toBe(false)

    fireEvent.click(screen.getByRole("radio", { name: "usage.view.records" }))
    await waitFor(() => expect(history.signal.aborted).toBe(true))
    expect(screen.queryByRole("alert")).not.toBeInTheDocument()
    unmount()
    expect(matching("usage-records").at(-1)!.signal.aborted).toBe(true)
    expect(matching("usage-summary").at(-1)!.signal.aborted).toBe(true)
  })

  it("cancels the old log detail when selecting another request and aborts on navigation", async () => {
    const { unmount } = mount(<LogsPage />)
    await completeInitial()
    await screen.findByText("first detail")
    fireEvent.click(screen.getByText("first detail"))
    await waitFor(() => expect(matching("logs/first")).toHaveLength(1))
    fireEvent.click(screen.getByText("second detail"))
    await waitFor(() => expect(matching("logs/second")).toHaveLength(1))
    expect(matching("logs/first")[0].signal.aborted).toBe(true)
    expect(screen.queryByRole("alert")).not.toBeInTheDocument()

    fireEvent.change(screen.getByLabelText("request id"), { target: { value: "filtered" } })
    fireEvent.click(screen.getByText("search"))
    await waitFor(() => expect(matching("logs")).toHaveLength(2))
    expect(matching("logs/second")[0].signal.aborted).toBe(true)
    unmount()
    expect(matching("logs")[1].signal.aborted).toBe(true)
  })
})

describe("overview refresh", () => {
  it("refreshes health, quota, usage and current-hour trend without hiding existing data", async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date("2026-01-01T12:10:00Z"))
    const { client, unmount } = mount(<OverviewPage />)
    await completeInitial()
    await act(async () => { await vi.advanceTimersByTimeAsync(1) })
    const dashboard = screen.getByTestId("dashboard")
    const paths = ["providers", "credentials", "usage", "quota-windows", "credential-cycles/query", "usage-trend"]
    for (const path of paths) expect(matching(path)).toHaveLength(1)
    expect(matching("credential-cycles/query")[0].body).toMatchObject({ current_only: true, include_estimate: false })

    await act(async () => { await vi.advanceTimersByTimeAsync(60_000) })
    for (const path of paths) expect(matching(path)).toHaveLength(2)
    expect(screen.getByTestId("dashboard")).toBe(dashboard)
    const previousTo = Number(matching("usage")[0].url.searchParams.get("to"))
    expect(Number(matching("usage")[1].url.searchParams.get("to"))).toBe(previousTo + 60)
    expect(matching("usage-trend")[1].url.search).toBe(matching("usage-trend")[0].url.search)

    await act(async () => {
      matching("providers")[1].respond([])
      matching("credentials")[1].respond([{ id: 1, health: "unhealthy" }])
      matching("quota-windows")[1].respond([{ quota_id: 1, cost_used: "90" }])
      matching("usage")[1].respond([{ provider_id: 1, requests: 2 }])
      matching("credential-cycles/query")[1].respond([{ id: 1, used_percent: "90" }])
      matching("usage-trend")[1].respond([{ requests: 2 }])
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(1) })
    expect(dashboard).toHaveTextContent('"health":"unhealthy"')
    expect(dashboard).toHaveTextContent('"cost_used":"90"')
    expect(dashboard).toHaveTextContent('"trend":[{"requests":2}]')
    expect(client.getQueryCache().findAll({ queryKey: ["usage"] })).toHaveLength(1)
    expect(client.getQueryCache().findAll({ queryKey: ["credential-cycles"] })).toHaveLength(1)

    await act(async () => { await vi.advanceTimersByTimeAsync(60_000) })
    unmount()
    for (const path of paths) expect(matching(path).at(-1)!.signal.aborted).toBe(true)
  })

  it("keeps the current chart visible while fetching the next hourly range", async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date("2026-01-01T12:59:30Z"))
    mount(<OverviewPage />)
    await completeInitial()
    await act(async () => { await vi.advanceTimersByTimeAsync(1) })
    const firstTo = Number(matching("usage-trend")[0].url.searchParams.get("to"))
    await act(async () => { await vi.advanceTimersByTimeAsync(60_000) })
    const latestTrend = matching("usage-trend").at(-1)!
    expect(Number(latestTrend.url.searchParams.get("to"))).toBe(firstTo + 3_600)
    expect(screen.getByTestId("dashboard")).toHaveTextContent('"trendLoading":false')
    expect(screen.getByTestId("dashboard")).toHaveTextContent(`"trendTo":${firstTo + 3_600}`)
  })
})
