import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { render, waitFor } from "@testing-library/react"
import { expect, it, vi } from "vitest"
import "@/i18n"
import type { UsageRecordDto } from "@/generated/UsageRecordDto"
import { UsageRecordDetail } from "./usage-record-detail"

it("releases cached log bodies when closing a record and aborts unfinished detail requests", async () => {
  const record: UsageRecordDto = {
    id: 1, request_id: "detail-cache", at: 10, provider_id: 1, credential_id: 1,
    user_id: null, user_key_id: null, operation: null, model: "model", input_tokens: 1,
    output_tokens: 1, cached_input_tokens: 0, metrics: {}, dimensions: {}, cost: "0",
    usage_source: "upstream", ended: "complete", latency_ms: 1,
  }
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  let pending: AbortSignal | undefined
  const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response(JSON.stringify({
    downstream: { request_id: record.request_id, method: "POST", path: "/v1/responses", request_body: "x".repeat(100_000), response_body: null }, upstream: [],
  }), { status: 200 })).mockImplementationOnce((_path, init) => new Promise((_resolve, reject) => {
    pending = init!.signal!
    pending.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true })
  }))
  const view = (selected: UsageRecordDto | null) => <QueryClientProvider client={client}><UsageRecordDetail record={selected} onClose={() => undefined} providers={[]} /></QueryClientProvider>
  const { rerender, unmount } = render(view(record))
  try {
    await waitFor(() => expect(client.getQueryData(["log-detail", record.request_id])).toBeDefined())
    rerender(view(null))
    await waitFor(() => expect(client.getQueryData(["log-detail", record.request_id])).toBeUndefined())
    rerender(view({ ...record, request_id: "pending" }))
    await waitFor(() => expect(pending).toBeDefined())
    rerender(view(null))
    await waitFor(() => expect(pending?.aborted).toBe(true))
  } finally { unmount(); client.clear(); fetch.mockRestore() }
})
