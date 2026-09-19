import type { CredentialHealthDto } from "@/generated/CredentialHealthDto"
import type { CredentialModelHealthDto } from "@/generated/CredentialModelHealthDto"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { afterEach, describe, expect, it, vi } from "vitest"
import { toast } from "sonner"
import "@/i18n"
import { CredentialHealthBadge } from "./credential-model-health"

const modelHealth = (model: string, health: CredentialHealthDto = "degraded"): CredentialModelHealthDto => ({
  model, health, observed_at: 1_789_171_200, response_status: 503, detail: "server_is_overloaded",
})
function view(models: CredentialModelHealthDto[], health: CredentialHealthDto = "degraded", client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } }), onClick = vi.fn()) {
  return <QueryClientProvider client={client}><div onClick={onClick}>
    <CredentialHealthBadge credentialId={7} health={health} models={models} observedAt={1_789_171_200} />
  </div></QueryClientProvider>
}

describe("CredentialHealthBadge", () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })

  it("shows model failures as partial issues and opens details without resetting or expanding the credential row", async () => {
    const fetchMock = vi.fn<typeof fetch>()
    vi.stubGlobal("fetch", fetchMock)
    const rowClick = vi.fn()
    render(view([modelHealth("gpt-5.4", "dead"), modelHealth("gpt-5.3", "healthy")], "dead", undefined, rowClick))
    const trigger = screen.getByRole("button", { name: "View account and model health: Some models affected (1)" })
    expect(trigger).toHaveTextContent("Some models affected (1)")
    expect(trigger).not.toHaveTextContent("Dead")
    await userEvent.setup().click(trigger)
    const detail = screen.getByRole("dialog", { name: "Health detail" })
    expect(within(detail).getByText("gpt-5.4")).toBeInTheDocument()
    expect(within(detail).getByText("503 · server_is_overloaded")).toBeInTheDocument()
    expect(within(detail).queryByText("gpt-5.3")).not.toBeInTheDocument()
    expect(fetchMock).not.toHaveBeenCalled()
    expect(rowClick).not.toHaveBeenCalled()
  })

  it("keeps account failures separate and does not count them as a model", async () => {
    render(view([modelHealth("gpt-5.4"), modelHealth("*", "dead"), modelHealth("")], "dead"))
    const trigger = screen.getByRole("button", { name: /View account and model health/ })
    expect(trigger).toHaveTextContent("Account: Dead")
    expect(trigger).toHaveTextContent("Some models affected (2)")
    await userEvent.setup().click(trigger)
    const entries = screen.getAllByRole("listitem")
    expect(entries[0]).toHaveTextContent("Account-wide")
    expect(within(screen.getByRole("dialog")).getByText("Unspecified model")).toBeInTheDocument()
    expect(screen.queryByText("*")).not.toBeInTheDocument()
  })

  it.each([
    ["gpt-5.4", "gpt-5.4"],
    ["", "Unspecified model"],
    ["*", "Account-wide"],
  ])("resets only the exact %j scope and refreshes credential health", async (model, label) => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal("fetch", fetchMock)
    const client = new QueryClient()
    client.setQueryData(["credentials"], [])
    client.setQueryData(["providers"], [])
    const rowClick = vi.fn()
    const user = userEvent.setup()
    render(view([modelHealth(model), modelHealth("other-model")], "degraded", client, rowClick))
    await user.click(screen.getByRole("button", { name: /View account and model health/ }))
    await user.click(screen.getByRole("button", { name: `Reset health for ${label}` }))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledOnce())
    expect(fetchMock.mock.calls[0]?.[0]).toBe("/admin/api/credentials/7/health-reset")
    expect(fetchMock.mock.calls[0]?.[1]?.method).toBe("POST")
    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toEqual({ model })
    await waitFor(() => expect(client.getQueryState(["credentials"])?.isInvalidated).toBe(true))
    expect(client.getQueryState(["providers"])?.isInvalidated).toBe(false)
    expect(screen.getByRole("button", { name: "Reset health for other-model" })).toBeInTheDocument()
    expect(rowClick).not.toHaveBeenCalled()
    client.clear()
  })

  it("requires the explicit account-wide action to clear all model and account records", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal("fetch", fetchMock)
    const user = userEvent.setup()
    render(view([modelHealth("gpt-5.4"), modelHealth("*")]))
    await user.click(screen.getByRole("button", { name: /View account and model health/ }))
    expect(fetchMock).not.toHaveBeenCalled()
    await user.click(screen.getByRole("button", { name: "Reset all health records for this account" }))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledOnce())
    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toEqual({})
  })

  it("makes every affected model actionable, including entries after the first eight", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal("fetch", fetchMock)
    const user = userEvent.setup()
    render(view(Array.from({ length: 10 }, (_, index) => modelHealth(`model-${index}`))))
    await user.click(screen.getByRole("button", { name: /Some models affected \(10\)/ }))
    expect(screen.getAllByRole("listitem")).toHaveLength(10)
    await user.click(screen.getByRole("button", { name: "Reset health for model-9" }))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledOnce())
    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toEqual({ model: "model-9" })
  })

  it("opens with the keyboard and restores focus after closing without making a request", async () => {
    const fetchMock = vi.fn<typeof fetch>()
    vi.stubGlobal("fetch", fetchMock)
    const user = userEvent.setup()
    render(view([modelHealth("gpt-5.4")]))
    const trigger = screen.getByRole("button", { name: /View account and model health/ })
    await user.tab()
    expect(trigger).toHaveFocus()
    await user.keyboard("{Enter}")
    expect(screen.getByRole("dialog", { name: "Health detail" })).toBeInTheDocument()
    await user.keyboard("{Escape}")
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument())
    expect(trigger).toHaveFocus()
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it("reports reset failures and retains health details", async () => {
    const errorToast = vi.spyOn(toast, "error")
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify({ error: { message: "Reset failed" } }), { status: 503 })))
    const user = userEvent.setup()
    render(view([modelHealth("gpt-5.4")]))
    await user.click(screen.getByRole("button", { name: /View account and model health/ }))
    await user.click(screen.getByRole("button", { name: "Reset health for gpt-5.4" }))
    await waitFor(() => expect(errorToast).toHaveBeenCalledWith("Reset failed"))
    expect(screen.getByText("503 · server_is_overloaded")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Reset health for gpt-5.4" })).toBeEnabled()
  })

  it("prevents concurrent resets while a scope reset is pending", async () => {
    let finish!: (response: Response) => void
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(() => new Promise((resolve) => { finish = resolve }))
    vi.stubGlobal("fetch", fetchMock)
    const user = userEvent.setup()
    render(view([modelHealth("gpt-5.4"), modelHealth("gpt-5.3")]))
    await user.click(screen.getByRole("button", { name: /View account and model health/ }))
    await user.click(screen.getByRole("button", { name: "Reset health for gpt-5.4" }))
    const otherReset = screen.getByRole("button", { name: "Reset health for gpt-5.3" })
    expect(otherReset).toBeDisabled()
    expect(screen.getByRole("button", { name: "Reset all health records for this account" })).toBeDisabled()
    await user.click(otherReset)
    expect(fetchMock).toHaveBeenCalledOnce()
    finish(new Response(null, { status: 204 }))
    await waitFor(() => expect(otherReset).toBeEnabled())
  })

  it("preserves healthy status and disables a full reset when no records exist", async () => {
    render(view([], "healthy"))
    const trigger = screen.getByRole("button", { name: "View account and model health: Healthy" })
    await userEvent.setup().click(trigger)
    expect(screen.getByText("No health issues recorded.")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Reset all health records for this account" })).toBeDisabled()
  })

  it("preserves disabled status even when stored model failures exist", () => {
    render(view([modelHealth("gpt-5.4")], "disabled"))
    const trigger = screen.getByRole("button", { name: /View account and model health/ })
    expect(trigger).toHaveTextContent("Disabled")
    expect(trigger).not.toHaveTextContent("Some models affected")
  })
})
