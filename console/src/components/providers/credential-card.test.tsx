import type { CredentialDto } from "@/generated/CredentialDto"
import type { QuotaSnapshot } from "@/generated/QuotaSnapshot"
import type { QuotaProbeResponse } from "@/generated/QuotaProbeResponse"
import { QueryClient, QueryClientProvider, useQuery } from "@tanstack/react-query"
import { act, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { afterEach, describe, expect, it, vi } from "vitest"
import "@/i18n"
import { CredentialCard } from "@/components/providers/credential-card"
import { CredentialList } from "@/components/providers/credential-list"
import { TooltipProvider } from "@/components/ui/tooltip"
import { credentials } from "@/api/control"

const credential: CredentialDto = {
  id: 7, provider_id: 3, label: "New credential", kind: "oauth",
  quota_capabilities: { probe: true, reset: false, top_up_url: null }, refresh_supported: false, version: 1, enabled: true, weight: 100,
  rpm_limit: null, tpm_limit: null, proxy_url: null, tls_fingerprint: null,
  invalid_tls_fingerprint: null, tls_fingerprint_error: null, health: "unknown",
  health_observed_at: null, health_response_status: null, health_detail: null, model_health: [],
}

function snapshot(observed: number | null = Date.now()): QuotaSnapshot {
  return {
    sources: [{ capability: { id: "balance", label: "Account balance", kinds: ["balance"], mode: "probe", support: "ready", reason: null, automatic: true }, attempted_at_ms: observed, observed_at_ms: observed, error: null, reset_credits: null }],
    entries: observed == null ? [] : [{ id: "balance:CNY", source_id: "balance", label: "CNY balance", subject: "account", model_scope: { kind: "all" }, observed_at_ms: observed,
      value: { kind: "balance", remaining: "110.00", unit: "CNY", availability: "available", components: [{ kind: "granted", amount: "10.00" }, { kind: "topped_up", amount: "100.00" }] } }],
  }
}
const response = (body: unknown) => new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } })
const probed = (value: QuotaSnapshot, credential_version = 1): QuotaProbeResponse => ({ credential_version, snapshot: value, windows: [], cycles: [], local_error: false, reset_credits: null, raw: "{}" })
function view(value = credential, client = new QueryClient({ defaultOptions: { queries: { retry: false } } })) {
  return <QueryClientProvider client={client}><TooltipProvider><CredentialCard credential={value} cycles={[]} cyclesLoading={false} cyclesError={false} /></TooltipProvider></QueryClientProvider>
}

describe("CredentialCard", () => {
  afterEach(() => vi.unstubAllGlobals())

  it("loads a saved snapshot before probing stale data and reuses fresh results on reopen", async () => {
    let resolveProbe!: (response: Response) => void
    const fetchMock = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(response(snapshot(null)))
      .mockImplementationOnce(() => new Promise((resolve) => { resolveProbe = resolve }))
    vi.stubGlobal("fetch", fetchMock)
    const component = view()
    const mounted = render(component)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    expect(fetchMock.mock.calls.map(([path]) => path)).toEqual(["/admin/api/credentials/7/quota", "/admin/api/credentials/7/quota-probe?lightweight=true"])
    expect(screen.getByRole("button", { name: "Fetching…" })).toBeDisabled()
    resolveProbe(response(probed(snapshot())))
    await screen.findByText("110.00 CNY")
    mounted.rerender(<></>)
    mounted.rerender(component)
    expect(screen.getByText("110.00 CNY")).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it.each(["snapshot", "probe"])("cancels an unfinished %s read when the quota card closes", async (stage) => {
    let signal: AbortSignal | null | undefined
    const fetchMock = vi.fn<typeof fetch>().mockImplementation((path, init) => {
      if (stage === "probe" && String(path).endsWith("/quota")) return Promise.resolve(response(snapshot(null)))
      signal = init?.signal
      return new Promise((_resolve, reject) => signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true }))
    })
    vi.stubGlobal("fetch", fetchMock)
    const { unmount } = render(view())
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(stage === "probe" ? 2 : 1))
    expect(signal?.aborted).toBe(false)
    unmount()
    expect(signal?.aborted).toBe(true)
  })

  it("invalidates affected cycle summaries without refreshing unrelated providers or credentials", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const ownProvider = ["credential-cycles", "providers", 3]
    const otherProvider = ["credential-cycles", "providers", 4]
    const ownHistory = ["credential-cycles", "history", { credential_id: 7 }]
    const otherHistory = ["credential-cycles", "history", { credential_id: 8 }]
    for (const key of [ownProvider, otherProvider, ownHistory, otherHistory]) client.setQueryData(key, [])
    vi.stubGlobal("fetch", vi.fn<typeof fetch>()
      .mockResolvedValueOnce(response(snapshot()))
      .mockResolvedValueOnce(response(probed(snapshot()))))
    render(view(credential, client))
    await screen.findByText("110.00 CNY")
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh" }))
    await waitFor(() => expect(client.getQueryState(ownProvider)?.isInvalidated).toBe(true))
    expect(client.getQueryState(ownHistory)?.isInvalidated).toBe(true)
    expect(client.getQueryState(otherProvider)?.isInvalidated).toBe(false)
    expect(client.getQueryState(otherHistory)?.isInvalidated).toBe(false)
  })

  it("keeps an older-version manual result out of the current credential cache", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    let resolveProbe!: (response: Response) => void
    const current = snapshot()
    current.entries[0].value = { kind: "balance", remaining: "220.00", unit: "CNY", availability: "available", components: [] }
    vi.stubGlobal("fetch", vi.fn<typeof fetch>()
      .mockResolvedValueOnce(response(snapshot()))
      .mockImplementationOnce(() => new Promise((resolve) => { resolveProbe = resolve }))
      .mockResolvedValueOnce(response(current)))
    const mounted = render(view(credential, client))
    await screen.findByText("110.00 CNY")
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh" }))
    mounted.rerender(view({ ...credential, version: 2 }, client))
    await screen.findByText("220.00 CNY")
    resolveProbe(response(probed(snapshot())))
    await waitFor(() => expect(client.getQueryData(["credential-quota-probe", 7, 1])).toBeDefined())
    expect(client.getQueryData(["credential-quota", 7, 2])).toEqual(current)
    expect(client.getQueryData(["credential-quota-probe", 7, 2])).toBeUndefined()
    expect(screen.getByText("220.00 CNY")).toBeInTheDocument()
  })

  it.each(["automatic", "manual"])("moves %s probe results to the actual credential version and rejects an older list read", async (mode) => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const initial = snapshot(mode === "automatic" ? null : Date.now())
    const current = snapshot(Date.now() + 1)
    current.entries[0].value = { kind: "balance", remaining: "220.00", unit: "CNY", availability: "available", components: [] }
    const result = probed(current, 2)
    let resolveProbe!: (value: Response) => void
    let resolveOldList!: (value: Response) => void
    let oldListSignal: AbortSignal | null | undefined
    let listCalls = 0
    const fetchMock = vi.fn<typeof fetch>().mockImplementation((path, init) => {
      if (String(path) === "/admin/api/credentials") {
        if (listCalls++ > 0) return Promise.resolve(response([{ ...credential, version: 2 }]))
        oldListSignal = init?.signal
        return new Promise((resolve) => { resolveOldList = resolve })
      }
      if (String(path).endsWith("/quota")) return Promise.resolve(response(initial))
      return new Promise((resolve) => { resolveProbe = resolve })
    })
    vi.stubGlobal("fetch", fetchMock)
    function CurrentCard() {
      const saved = useQuery({ queryKey: ["credentials"], queryFn: ({ signal }) => credentials(signal), initialData: [credential], staleTime: Infinity })
      return <TooltipProvider><CredentialCard credential={saved.data[0]} cycles={[]} cyclesLoading={false} cyclesError={false} /></TooltipProvider>
    }
    render(<QueryClientProvider client={client}><CurrentCard /></QueryClientProvider>)
    if (mode === "manual") {
      await screen.findByText("110.00 CNY")
      await userEvent.setup().click(screen.getByRole("button", { name: "Refresh" }))
    }
    await waitFor(() => expect(resolveProbe).toBeDefined())
    void client.invalidateQueries({ queryKey: ["credentials"] })
    await waitFor(() => expect(resolveOldList).toBeDefined())
    resolveProbe(response(result))
    await screen.findByText("220.00 CNY")
    await waitFor(() => expect(listCalls).toBe(2))
    expect(oldListSignal?.aborted).toBe(true)
    await act(async () => resolveOldList(response([credential])))
    expect(client.getQueryData(["credentials"])).toEqual([{ ...credential, version: 2 }])
    expect(client.getQueryData(["credential-quota", 7, 2])).toEqual(current)
    expect(client.getQueryData(["credential-quota-probe", 7, 2])).toEqual(result)
    expect(client.getQueryData(["credential-quota", 7, 1])).toEqual(initial)
    expect(client.getQueryData(["credential-quota-probe", 7, 1])).not.toEqual(result)
    expect(fetchMock.mock.calls.filter(([path]) => String(path).includes("/quota-probe"))).toHaveLength(1)
  })

  it("preserves a newer snapshot and probe when an older request reaches the same rotated version", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    let resolveProbe!: (value: Response) => void
    const initial = snapshot(Date.now())
    const older = snapshot(Date.now() + 1)
    const current = snapshot(Date.now() + 2)
    current.entries[0].value = { kind: "balance", remaining: "330.00", unit: "CNY", availability: "available", components: [] }
    const currentProbe = probed(current, 2)
    vi.stubGlobal("fetch", vi.fn<typeof fetch>()
      .mockResolvedValueOnce(response(initial))
      .mockImplementationOnce(() => new Promise((resolve) => { resolveProbe = resolve })))
    const mounted = render(view(credential, client))
    await screen.findByText("110.00 CNY")
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh" }))
    client.setQueryData(["credentials"], [{ ...credential, version: 3 }])
    client.setQueryData(["credential-quota", 7, 2], current)
    client.setQueryData(["credential-quota-probe", 7, 2], currentProbe)
    mounted.rerender(view({ ...credential, version: 2 }, client))
    await screen.findByText("330.00 CNY")
    resolveProbe(response(probed(older, 2)))
    await waitFor(() => expect(screen.getByRole("button", { name: "Refresh" })).toBeEnabled())
    expect(client.getQueryData(["credentials"])).toEqual([{ ...credential, version: 3 }])
    expect(client.getQueryData(["credential-quota", 7, 2])).toEqual(current)
    expect(client.getQueryData(["credential-quota-probe", 7, 2])).toEqual(currentProbe)
    expect(client.getQueryData(["credential-quota-probe", 7, 1])).toBeUndefined()
  })

  it("keeps a rotated late result bound to the original account and provider after navigating away", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const other = { ...credential, id: 8, provider_id: 4, version: 9 }
    const current = snapshot(Date.now() + 1)
    current.entries[0].value = { kind: "balance", remaining: "440.00", unit: "CNY", availability: "available", components: [] }
    client.setQueryData(["credentials"], [credential, other])
    client.setQueryData(["credential-cycles", "providers", 3], [])
    client.setQueryData(["credential-cycles", "providers", 4], [])
    let resolveProbe!: (value: Response) => void
    vi.stubGlobal("fetch", vi.fn<typeof fetch>()
      .mockResolvedValueOnce(response(snapshot()))
      .mockImplementationOnce(() => new Promise((resolve) => { resolveProbe = resolve }))
      .mockResolvedValueOnce(response(current)))
    const mounted = render(view(credential, client))
    await screen.findByText("110.00 CNY")
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh" }))
    mounted.rerender(view(other, client))
    await screen.findByText("440.00 CNY")
    const result = probed(snapshot(), 2)
    resolveProbe(response(result))
    await waitFor(() => expect(client.getQueryData(["credential-quota-probe", 7, 2])).toEqual(result))
    expect(client.getQueryData(["credentials"])).toEqual([{ ...credential, version: 2 }, other])
    expect(client.getQueryData(["credential-quota", 8, 9])).toEqual(current)
    expect(client.getQueryData(["credential-quota-probe", 8, 9])).toBeUndefined()
    expect(client.getQueryState(["credential-cycles", "providers", 3])?.isInvalidated).toBe(true)
    expect(client.getQueryState(["credential-cycles", "providers", 4])?.isInvalidated).toBe(false)
  })

  it("keeps a same-millisecond success when an in-flight failed probe arrives later", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const attempted = Date.now()
    const current = snapshot(attempted)
    current.entries[0].value = { kind: "balance", remaining: "330.00", unit: "CNY", availability: "available", components: [] }
    const failed = snapshot(attempted)
    failed.sources[0].error = { code: "unauthorized", message: "Late quota error" }
    const currentProbe = probed(current)
    let resolveProbe!: (value: Response) => void
    vi.stubGlobal("fetch", vi.fn<typeof fetch>()
      .mockResolvedValueOnce(response(snapshot(attempted - 1)))
      .mockImplementationOnce(() => new Promise((resolve) => { resolveProbe = resolve })))
    render(view(credential, client))
    await screen.findByText("110.00 CNY")
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh" }))
    act(() => {
      client.setQueryData(["credential-quota", 7, 1], current)
      client.setQueryData(["credential-quota-probe", 7, 1], currentProbe)
    })
    await screen.findByText("330.00 CNY")
    resolveProbe(response(probed(failed)))
    await waitFor(() => expect(screen.getByRole("button", { name: "Refresh" })).toBeEnabled())
    expect(client.getQueryData(["credential-quota", 7, 1])).toEqual(current)
    expect(client.getQueryData(["credential-quota-probe", 7, 1])).toEqual(currentProbe)
    expect(screen.queryByText("Late quota error")).not.toBeInTheDocument()
    expect(screen.getByText("330.00 CNY")).toBeInTheDocument()
  })

  it("does not let an older snapshot read overwrite a completed probe", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    let resolveSnapshot!: (response: Response) => void
    let oldReadSignal: AbortSignal | null | undefined
    const initial = snapshot()
    const current = snapshot(initial.sources[0].observed_at_ms! + 1)
    current.entries[0].value = { kind: "balance", remaining: "220.00", unit: "CNY", availability: "available", components: [] }
    vi.stubGlobal("fetch", vi.fn<typeof fetch>()
      .mockResolvedValueOnce(response(initial))
      .mockImplementationOnce((_path, init) => {
        oldReadSignal = init?.signal
        return new Promise((resolve) => { resolveSnapshot = resolve })
      })
      .mockResolvedValueOnce(response(probed(current))))
    const mounted = render(view(credential, client))
    await screen.findByText("110.00 CNY")
    const otherSignals: AbortSignal[] = []
    for (const key of [["credential-quota", 8, 1], ["credential-quota", 7, 2]]) {
      void client.fetchQuery({ queryKey: key, queryFn: ({ signal }) => new Promise((_resolve, reject) => {
        otherSignals.push(signal)
        signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true })
      }) }).catch(() => {})
    }
    void client.invalidateQueries({ queryKey: ["credential-quota", 7, 1], exact: true })
    await waitFor(() => expect(oldReadSignal).toBeDefined())
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh" }))
    await screen.findByText("220.00 CNY")
    await act(async () => resolveSnapshot(response(initial)))
    expect(client.getQueryData(["credential-quota", 7, 1])).toEqual(current)
    expect(oldReadSignal?.aborted).toBe(true)
    expect(otherSignals).toHaveLength(2)
    expect(otherSignals.every((signal) => !signal.aborted)).toBe(true)
    expect(screen.getByText("220.00 CNY")).toBeInTheDocument()
    mounted.unmount()
    client.clear()
  })

  it("keeps historical cycles available when a window source has no current entries", async () => {
    const value = snapshot()
    value.entries = []
    value.sources[0].capability = { ...value.sources[0].capability, kinds: ["window"], mode: "response" }
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValueOnce(response(value))
    vi.stubGlobal("fetch", fetchMock)
    render(view())
    expect(await screen.findByRole("button", { name: "Previous rounds in the last year" })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledOnce()
  })

  it("preserves an API Key's saved balance when refresh fails and reports per-source errors", async () => {
    const fresh = snapshot()
    const failed = snapshot(fresh.sources[0].attempted_at_ms! + 1)
    failed.sources[0].error = { code: "unauthorized", message: "Upstream returned 401" }
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValueOnce(response(fresh)).mockResolvedValueOnce(response(probed(failed)))
    vi.stubGlobal("fetch", fetchMock)
    render(view({ ...credential, kind: "api_key", quota_capabilities: null }))
    await screen.findByText("110.00 CNY")
    expect(fetchMock).toHaveBeenCalledOnce()
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh" }))
    expect(await screen.findByText("Refresh failed; showing the last successful result.")).toBeInTheDocument()
    expect(screen.getByText("Upstream returned 401")).toBeInTheDocument()
    expect(screen.getByText("110.00 CNY")).toBeInTheDocument()
    expect(screen.queryByText("Upstream reports insufficient balance")).not.toBeInTheDocument()
    expect(fetchMock.mock.calls[1]?.[0]).toBe("/admin/api/credentials/7/quota-probe?force=true&lightweight=true")
  })

  it("keeps unsupported and response-observed quota visible without active probing", async () => {
    const value = snapshot(null)
    value.sources[0].capability = { ...value.sources[0].capability, support: "unsupported", mode: "unavailable", reason: "No documented endpoint" }
    value.sources.push({ ...value.sources[0], capability: { ...value.sources[0].capability, id: "headers", label: "Request limits", mode: "response", support: "ready", reason: null } })
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(response(value))
    vi.stubGlobal("fetch", fetchMock)
    render(view({ ...credential, kind: "api_key", quota_capabilities: null }))
    expect(await screen.findByText("Quota queries are not supported yet.")).toBeInTheDocument()
    expect(screen.getByText("Updates with requests")).toBeInTheDocument()
    expect(screen.getByText("No quota observed yet.")).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "Refresh" })).not.toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledOnce()
  })

  it("separates unknown balances, unlimited budgets, rate limits and dated usage reports", async () => {
    const value = snapshot()
    const base = value.entries[0]
    value.entries = [
      { ...base, value: { kind: "balance", remaining: null, unit: "CNY", availability: "unknown", components: [] } },
      { ...base, id: "budget", label: "Key budget", subject: "key", value: { kind: "budget", used: "12", remaining: null, limit: null, unlimited: true, used_percent: null, unit: "USD", period_start: null, period_end: null } },
      { ...base, id: "rate", label: "Request rate", value: { kind: "rate_limit", used: "10", remaining: "90", limit: "100", unlimited: false, used_percent: "10", unit: "requests", period_start: null, period_end: 2_000_000_000 } },
      { ...base, id: "report", label: "Monthly usage", value: { kind: "usage_report", used: "19.123456", unit: "USD", period_start: 1_999_900_000, period_end: 2_000_000_000 } },
      { ...base, id: "empty", label: "USD balance", value: { kind: "balance", remaining: "0", unit: "USD", availability: "unavailable", components: [] } },
    ]
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(response(value)))
    render(view())
    expect(await screen.findByText("Availability unknown")).toBeInTheDocument()
    expect(screen.getByText("Unlimited")).toBeInTheDocument()
    expect(screen.getByText("90 requests")).toBeInTheDocument()
    expect(screen.getByText("19.123456 USD")).toBeInTheDocument()
    expect(screen.getByText(/Usage from/)).toBeInTheDocument()
    expect(screen.getByText("Upstream reports insufficient balance")).toBeInTheDocument()
    expect(screen.queryByText("0 CNY")).not.toBeInTheDocument()
  })

  it("marks old snapshots and only probes manual sources after Refresh", async () => {
    const old = snapshot(Date.now() - 601_000)
    old.sources[0].capability.automatic = false
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValueOnce(response(old)).mockResolvedValueOnce(response(probed(snapshot())))
    vi.stubGlobal("fetch", fetchMock)
    render(view())
    expect(await screen.findByText("Snapshot older than 10 minutes")).toBeInTheDocument()
    expect(screen.getByText("110.00 CNY")).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledOnce()
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh" }))
    await waitFor(() => expect(screen.queryByText("Snapshot older than 10 minutes")).not.toBeInTheDocument())
    expect(fetchMock.mock.calls[1]?.[0]).toBe("/admin/api/credentials/7/quota-probe?force=true&lightweight=true")
  })

  it("keeps persisted reset credits visible and refreshes their count after redemption", async () => {
    const initial = snapshot()
    initial.sources[0].reset_credits = { available_count: 2, expires_at: 2_000_000_000 }
    const redeemed = snapshot()
    redeemed.sources[0].reset_credits = { available_count: 0, expires_at: null }
    const fetchMock = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(response(initial))
      .mockResolvedValueOnce(response({ outcome: "reset", windows_reset: 1 }))
      .mockResolvedValueOnce(response(probed(redeemed)))
    vi.stubGlobal("fetch", fetchMock)
    const user = userEvent.setup()
    render(view({ ...credential, quota_capabilities: { probe: true, reset: true, top_up_url: null } }))
    const credits = screen.getByRole("region", { name: "Reset credits" })
    expect(within(credits).getByText("—")).toBeInTheDocument()
    expect(await within(credits).findByText("2")).toBeInTheDocument()
    expect(within(credits).getByText(/Expires/)).toBeInTheDocument()
    await user.click(within(credits).getByRole("button", { name: "Consume reset credit" }))
    expect(fetchMock).toHaveBeenCalledOnce()
    await user.click(within(screen.getByRole("alertdialog")).getByRole("button", { name: "Consume credit" }))
    expect(await within(credits).findByText("0")).toBeInTheDocument()
    expect(fetchMock.mock.calls[1]?.[0]).toBe("/admin/api/credentials/7/quota-reset")
    expect(within(credits).getByRole("button", { name: "Consume reset credit" })).toBeDisabled()
  })

  it("retains expandable quota details for ordinary API Keys", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(async (path) => response(String(path).endsWith("/quota") ? snapshot() : []))
    vi.stubGlobal("fetch", fetchMock)
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(<QueryClientProvider client={client}><TooltipProvider>
      <CredentialList providerId={3} presets={[]} credentials={[{ ...credential, kind: "api_key", quota_capabilities: null }]} cyclesByCredential={new Map()}
        credentialsLoading={false} credentialsError={false} cyclesLoading={false} cyclesError={false}
        savingCredentialId={null} onSave={vi.fn()} />
    </TooltipProvider></QueryClientProvider>)
    expect(fetchMock).not.toHaveBeenCalled()
    await userEvent.setup().click(screen.getByRole("row", { name: /New credential/ }))
    expect(await within(screen.getByRole("table")).findByText("110.00 CNY")).toBeInTheDocument()
    expect(fetchMock.mock.calls.map(([path]) => path)).toContain("/admin/api/credentials/7/quota")
    expect(fetchMock.mock.calls.map(([path]) => path)).not.toContain("/admin/api/credentials/7/quota-probe")
  })
})
