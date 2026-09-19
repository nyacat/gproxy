import type { CredentialDto } from "@/generated/CredentialDto"
import type { CredentialRefreshResponse } from "@/generated/CredentialRefreshResponse"
import type { ChannelDto } from "@/generated/ChannelDto"
import { QueryClient, QueryClientProvider, useQuery } from "@tanstack/react-query"
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { credentials } from "@/api/control"
import { CredentialRowActions } from "@/components/providers/credential-row-actions"
import "@/i18n"

const notifications = vi.hoisted(() => ({ success: vi.fn(), info: vi.fn(), error: vi.fn() }))
vi.mock("sonner", () => ({ toast: notifications }))

const credential: CredentialDto = {
  id: 7, provider_id: 3, label: "OAuth account", kind: "oauth", refresh_supported: true,
  quota_capabilities: null, version: 1, enabled: true, weight: 100,
  rpm_limit: null, tpm_limit: null, proxy_url: null, tls_fingerprint: null,
  invalid_tls_fingerprint: null, tls_fingerprint_error: null, health: "unknown",
  health_observed_at: null, health_response_status: null, health_detail: null, model_health: [],
}
const channel: ChannelDto = {
  id: "codex", display_name: "Codex", supports: [], routing_defaults: [], login: null,
  provider_fields: [], quota_fields: [],
  credential_fields: ["access_token", "refresh_token"].map((key) => ({
    key, i18n_key: key, control: "secret", required: false, advanced: false, default_value: null, options: [],
  })),
  endpoint_kinds: [], traffic_policy: { request_headers: [], response_headers: [], request_query: [] },
}
const response = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } })
const refreshed = (status: CredentialRefreshResponse["refresh_token_status"] = "updated", version = 2): CredentialRefreshResponse => ({
  credential_id: 7, credential_version: version, refresh_token_status: status,
})
const client = () => new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
const actions = (value = credential, saving = false) => <CredentialRowActions credential={value} channel={channel} presets={[]} saving={saving} onSave={vi.fn()} />
const view = (value = credential, cache = client(), count = 1, saving = false) => (
  <QueryClientProvider client={cache}>{Array.from({ length: count }, (_, index) => <div key={index}>{actions(value, saving)}</div>)}</QueryClientProvider>
)

describe("manual OAuth refresh", () => {
  beforeEach(() => vi.clearAllMocks())
  afterEach(() => vi.unstubAllGlobals())

  it("only offers refresh when the server reports a usable refresh credential", () => {
    const fetchMock = vi.fn<typeof fetch>()
    vi.stubGlobal("fetch", fetchMock)
    const cache = client()
    const mounted = render(view({ ...credential, refresh_supported: false }, cache))
    expect(screen.queryByRole("button", { name: /Refresh OAuth token/ })).not.toBeInTheDocument()
    mounted.rerender(view({ ...credential, enabled: false }, cache))
    expect(screen.getByRole("button", { name: /Refresh OAuth token/ })).toBeDisabled()
    mounted.rerender(view(credential, cache, 1, true))
    expect(screen.getByRole("button", { name: /Refresh OAuth token/ })).toBeDisabled()
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it.each([
    ["updated", "success", "OAuth token refreshed and the new refresh token was saved."],
    ["unchanged", "success", "OAuth token refreshed; the refresh token did not change."],
    ["not_returned", "info", "OAuth token refreshed, but upstream returned no new refresh token; the existing token was kept."],
    ["not_applicable", "success", "OAuth token refreshed."],
    ["updated_elsewhere", "info", "This credential was updated by another request."],
  ] as const)("reports %s accurately without receiving or resending secret tokens", async (status, notification, message) => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(response(refreshed(status)))
    vi.stubGlobal("fetch", fetchMock)
    const selected = vi.fn()
    render(<div onClick={selected}>{view({ ...credential, version: 0 })}</div>)
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh OAuth token: OAuth account" }))
    await waitFor(() => expect(notifications[notification]).toHaveBeenCalledWith(message))
    expect(fetchMock).toHaveBeenCalledOnce()
    expect(fetchMock.mock.calls[0][0]).toBe("/admin/api/credentials/7/refresh")
    expect(fetchMock.mock.calls[0][1]).toMatchObject({ method: "POST", body: '{"version":0}', credentials: "same-origin" })
    expect(selected).not.toHaveBeenCalled()
    expect(notifications.error).not.toHaveBeenCalled()
    if (notification === "info") expect(notifications.success).not.toHaveBeenCalled()
  })

  it("shares the pending state and rejects immediate duplicate clicks across desktop and mobile actions", async () => {
    let resolveRefresh!: (value: Response) => void
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(() => new Promise((resolve) => { resolveRefresh = resolve }))
    vi.stubGlobal("fetch", fetchMock)
    const cache = client()
    const mounted = render(view(credential, cache, 2))
    const buttons = screen.getAllByRole("button", { name: /Refresh OAuth token/ })
    act(() => { fireEvent.click(buttons[0]); fireEvent.click(buttons[1]) })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledOnce())
    expect(screen.getAllByRole("button", { name: /Refreshing OAuth token/ })).toHaveLength(2)
    expect(screen.getAllByRole("button").every((button) => button.hasAttribute("disabled"))).toBe(true)
    mounted.rerender(view({ ...credential, version: 2 }, cache, 2))
    expect(screen.getAllByRole("button", { name: /Refreshing OAuth token/ }).every((button) => button.hasAttribute("disabled"))).toBe(true)
    resolveRefresh(response(refreshed()))
    await waitFor(() => expect(screen.getAllByRole("button", { name: /Refresh OAuth token/ }).every((button) => !button.hasAttribute("disabled"))).toBe(true))
    expect(fetchMock).toHaveBeenCalledOnce()
    expect(notifications.success).toHaveBeenCalledOnce()
  })

  it("keeps a completed request bound to its original account and invalidates only affected quota scopes", async () => {
    let resolveRefresh!: (value: Response) => void
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(() => new Promise((resolve) => { resolveRefresh = resolve }))
    vi.stubGlobal("fetch", fetchMock)
    const cache = client()
    const other = { ...credential, id: 8, provider_id: 4, version: 4 }
    cache.setQueryData(["credentials"], [credential, other])
    const affected = [
      ["credential-quota", 7, 1], ["credential-quota-probe", 7, 1],
      ["credential-cycles", "providers", 3], ["credential-cycles", "history", { credential_id: 7 }],
    ]
    const untouched = [
      ["credential-quota", 7, 3], ["credential-quota", 8, 4], ["credential-quota-probe", 8, 4],
      ["credential-cycles", "providers", 4], ["credential-cycles", "history", { credential_id: 8 }],
    ]
    for (const key of [...affected, ...untouched]) cache.setQueryData(key, [])
    const mounted = render(view(credential, cache))
    await userEvent.setup().click(screen.getByRole("button", { name: /Refresh OAuth token/ }))
    mounted.rerender(view(other, cache))
    resolveRefresh(response(refreshed()))
    await waitFor(() => expect(notifications.success).toHaveBeenCalledOnce())
    expect(cache.getQueryData(["credentials"])).toEqual([{ ...credential, version: 2 }, other])
    for (const key of affected) expect(cache.getQueryState(key)?.isInvalidated).toBe(true)
    for (const key of untouched) expect(cache.getQueryState(key)?.isInvalidated).toBe(false)
    expect(fetchMock.mock.calls[0][0]).toBe("/admin/api/credentials/7/refresh")
    expect(fetchMock.mock.calls[0][1]?.body).toBe('{"version":1}')
  })

  it("cancels old list and snapshot reads so late data cannot restore the pre-refresh version", async () => {
    const cache = client()
    cache.setQueryData(["credentials"], [credential])
    cache.setQueryData(["credential-quota", 7, 1], { saved: true })
    const oldReads = [["credentials"], ["credential-quota", 7, 1]]
    const signals: AbortSignal[] = []
    const completions: Array<(value: unknown) => void> = []
    for (const key of oldReads) {
      void cache.fetchQuery({ queryKey: key, queryFn: ({ signal }) => {
        signals.push(signal)
        return new Promise((resolve) => completions.push(resolve))
      } }).catch(() => {})
    }
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(response(refreshed())))
    render(view(credential, cache))
    await userEvent.setup().click(screen.getByRole("button", { name: /Refresh OAuth token/ }))
    await waitFor(() => expect(notifications.success).toHaveBeenCalledOnce())
    expect(signals.every((signal) => signal.aborted)).toBe(true)
    await act(async () => { completions[0]([credential]); completions[1]({ obsolete: true }) })
    expect(cache.getQueryData(["credentials"])).toEqual([{ ...credential, version: 2 }])
    expect(cache.getQueryData(["credential-quota", 7, 1])).toEqual({ saved: true })
  })

  it("does not lower a version that another request advanced while manual refresh was running", async () => {
    const cache = client()
    cache.setQueryData(["credentials"], [{ ...credential, version: 3 }])
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(response(refreshed())))
    render(view(credential, cache))
    await userEvent.setup().click(screen.getByRole("button", { name: /Refresh OAuth token/ }))
    await waitFor(() => expect(notifications.success).toHaveBeenCalledOnce())
    expect(cache.getQueryData(["credentials"])).toEqual([{ ...credential, version: 3 }])
  })

  it.each([409, 502])("reports HTTP %s without claiming a successful refresh or inventing a new version", async (status) => {
    const cache = client()
    cache.setQueryData(["credentials"], [credential])
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(response({ error: { message: "Refresh could not complete." } }, status)))
    render(view(credential, cache))
    await userEvent.setup().click(screen.getByRole("button", { name: /Refresh OAuth token/ }))
    await waitFor(() => expect(notifications.error).toHaveBeenCalledWith("Refresh could not complete."))
    await waitFor(() => expect(screen.getByRole("button", { name: /Refresh OAuth token/ })).toBeEnabled())
    expect(notifications.success).not.toHaveBeenCalled()
    expect(notifications.info).not.toHaveBeenCalled()
    expect(cache.getQueryData(["credentials"])).toEqual([credential])
    expect(cache.getQueryState(["credentials"])?.isInvalidated).toBe(true)
  })

  it("discards an open editor's revealed secret after the credential version changes", async () => {
    function CachedActions() {
      const saved = useQuery({ queryKey: ["credentials"], queryFn: ({ signal }) => credentials(signal), initialData: [credential], staleTime: Infinity })
      return actions(saved.data[0])
    }
    const fetchMock = vi.fn<typeof fetch>().mockImplementation((path) => {
      if (String(path).endsWith("/reveal")) return Promise.resolve(response({ secret: { access_token: "old access", refresh_token: "old refresh" } }))
      if (String(path).endsWith("/refresh")) return Promise.resolve(response(refreshed()))
      return Promise.resolve(response([{ ...credential, version: 2 }]))
    })
    vi.stubGlobal("fetch", fetchMock)
    render(<QueryClientProvider client={client()}><CachedActions /></QueryClientProvider>)
    await userEvent.setup().click(screen.getByRole("button", { name: "Edit: OAuth account" }))
    await waitFor(() => expect(screen.getByRole("textbox", { name: "Credential JSON" })).toHaveValue(JSON.stringify({ access_token: "old access", refresh_token: "old refresh" }, null, 2)))
    fireEvent.click(screen.getByRole("button", { name: /Refresh OAuth token/, hidden: true }))
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument())
    expect(screen.queryByDisplayValue(/old refresh/)).not.toBeInTheDocument()
  })
})
