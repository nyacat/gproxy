import { useCallback, useMemo, useRef, useState } from "react"
import { readPageSize } from "@/components/data-table-state"
import { hashKey, useQueries, useQuery, useQueryClient } from "@tanstack/react-query"
import { useTranslation } from "react-i18next"
import type { UsageRecordQueryDto } from "@/generated/UsageRecordQueryDto"
import { usageRecords, usageSummary } from "@/api/observability"
import { credentials as fetchCredentials, providers as fetchProviders } from "@/api/control"
import { userKeys as fetchUserKeys, users as fetchUsers } from "@/api/identity"
import { PageLayout } from "@/components/page-layout"
import type { PageSize } from "@/components/data-table-pagination"
import { UsageExplorer } from "@/components/usage/usage-explorer"
import { QuotaHistoryPage } from "@/components/usage/quota-history-page"
import { ObservabilityTabs } from "@/components/observability-tabs"
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group"

type UsageView = "records" | "quotas"
const EMPTY: never[] = []

const now = () => Math.floor(Date.now() / 1000)
const usageFilter = (query: UsageRecordQueryDto) => ({ ...query, page: null, page_size: null })
const recordsKey = (query: UsageRecordQueryDto) => ["usage-records", usageFilter(query), query.page, query.page_size] as const

function initialQuery(): UsageRecordQueryDto {
  const to = now()
  return { from: to - 7 * 86_400, to, user_key_id: null, user_id: null, provider_id: null, credential_id: null, model: null, request_id: null, operation: null, usage_source: null, ended: null, page: 1, page_size: readPageSize("usage-records", 10), include_total: false }
}

export function UsagePage() {
  const { t } = useTranslation()
  const client = useQueryClient()
  const [view, setView] = useState<UsageView>("records")
  const [draft, setDraft] = useState<UsageRecordQueryDto>(initialQuery)
  const [query, setQuery] = useState<UsageRecordQueryDto>(draft)
  const onPage = useCallback((page: number) => setQuery((current) => ({ ...current, page })), [])
  const onPageSize = useCallback((page_size: PageSize) => setQuery((current) => ({ ...current, page: 1, page_size })), [])
  // Same rule as the request audit: an unpinned end of the range follows the clock.
  const pinnedTo = useRef(false)
  const editDraft: typeof setDraft = (update) => setDraft((previous) => {
    const next = typeof update === "function" ? update(previous) : update
    if (next.to !== previous.to) pinnedTo.current = true
    return next
  })
  const apply = () => {
    const to = pinnedTo.current ? draft.to : now()
    const next = { ...draft, to, page: 1, page_size: query.page_size }
    setDraft((value) => ({ ...value, to }))
    setQuery(next)
    if (view === "records") {
      // Explicit Apply refreshes both reads, including when page 1 is already
      // cached or only pagination changed. Inactive matches become stale and
      // are fetched when the new observers mount.
      void client.invalidateQueries({ queryKey: recordsKey(next), exact: true })
      void client.invalidateQueries({ queryKey: ["usage-summary", usageFilter(next)], exact: true })
    } else {
      void client.invalidateQueries({ queryKey: ["credential-cycles", "page"] })
    }
  }
  const filter = usageFilter(query)
  // Detach the inactive view's observers so in-flight requests can be aborted.
  const records = useQuery({
    queryKey: view === "records" ? recordsKey(query) : ["usage-records", null],
    queryFn: ({ signal }) => usageRecords(query, signal),
    // Retain a page while navigating within its filter only. Another filter's
    // rows must never be paired with the new summary or exposed as its result.
    placeholderData: (previous, previousQuery) => hashKey([previousQuery?.queryKey[1]]) === hashKey([filter]) ? previous : undefined,
    enabled: view === "records",
  })
  const [summary, credentialQuery, providerQuery, userQuery, keyQuery] = useQueries({ queries: [
    { queryKey: ["usage-summary", view === "records" ? filter : null], queryFn: ({ signal }) => usageSummary(query, signal), enabled: view === "records" },
    { queryKey: ["credentials"], queryFn: ({ signal }) => fetchCredentials(signal) },
    { queryKey: ["providers"], queryFn: ({ signal }) => fetchProviders(signal) },
    { queryKey: ["users"], queryFn: ({ signal }) => fetchUsers(signal), enabled: view === "records" },
    { queryKey: ["user-keys"], queryFn: ({ signal }) => fetchUserKeys(signal), enabled: view === "records" },
  ] })
  const totalRequests = summary.data?.requests
  const recordPage = useMemo(() => records.data
    ? { ...records.data, total: records.data.total ?? totalRequests ?? null }
    : { items: EMPTY, total: null, page: 1, page_size: 10, has_more: false }, [records.data, totalRequests])
  return (
    <PageLayout title={t("nav.usage")} description={t("usage.description")}>
      <ObservabilityTabs value="usage" />
      <ToggleGroup type="single" variant="outline" size="sm" spacing={0} value={view} aria-label={t("usage.view.label")} onValueChange={(next) => { if (next) setView(next as UsageView) }}>
        <ToggleGroupItem value="records">{t("usage.view.records")}</ToggleGroupItem>
        <ToggleGroupItem value="quotas">{t("usage.view.quotas")}</ToggleGroupItem>
      </ToggleGroup>
        <UsageExplorer
          view={view}
          draft={draft} onDraft={editDraft}
          onApply={apply}
          onReset={() => { const next = initialQuery(); pinnedTo.current = false; setDraft(next); setQuery(next) }}
          page={recordPage}
          summary={summary.data ?? null} summaryError={Boolean(summary.error)} pending={records.isFetching}
          loading={records.isLoading} error={Boolean(records.error)}
          onPage={onPage} onPageSize={onPageSize}
          credentials={credentialQuery.data ?? EMPTY} providers={providerQuery.data ?? EMPTY}
          users={userQuery.data ?? EMPTY} keys={keyQuery.data ?? EMPTY}
        >
          <QuotaHistoryPage
            range={{ from: query.from, to: query.to, credential_id: query.credential_id, provider_id: query.provider_id }}
            providers={providerQuery.data ?? []} credentials={credentialQuery.data ?? []}
            loading={providerQuery.isLoading || credentialQuery.isLoading}
            error={providerQuery.isError || credentialQuery.isError}
          />
        </UsageExplorer>
    </PageLayout>
  )
}
