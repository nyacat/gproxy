import { useCallback, useMemo, useRef, useState } from "react"
import { readPageSize } from "@/components/data-table-state"
import { hashKey, keepPreviousData, useQueries, useQuery } from "@tanstack/react-query"
import { useTranslation } from "react-i18next"
import type { UsageRecordQueryDto } from "@/generated/UsageRecordQueryDto"
import { queryCredentialCycles, usageRecords, usageSummary } from "@/api/observability"
import { credentials as fetchCredentials, providers as fetchProviders } from "@/api/control"
import { userKeys as fetchUserKeys, users as fetchUsers } from "@/api/identity"
import { PageLayout } from "@/components/page-layout"
import type { PageSize } from "@/components/data-table-pagination"
import { UsageExplorer } from "@/components/usage/usage-explorer"
import { QuotaHistory } from "@/components/usage/quota-history"
import { ObservabilityTabs } from "@/components/observability-tabs"
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group"

type UsageView = "records" | "quotas"
const EMPTY: never[] = []

const now = () => Math.floor(Date.now() / 1000)

function initialQuery(): UsageRecordQueryDto {
  const to = now()
  return { from: to - 7 * 86_400, to, user_key_id: null, user_id: null, provider_id: null, credential_id: null, model: null, request_id: null, operation: null, usage_source: null, ended: null, page: 1, page_size: readPageSize("usage-records", 10), include_total: false }
}

export function UsagePage() {
  const { t } = useTranslation()
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
    if (view === "records" && hashKey([next]) === hashKey([query])) {
      void records.refetch()
      void summary.refetch()
    } else if (view === "quotas" && next.from === query.from && next.to === query.to
      && next.provider_id === query.provider_id && next.credential_id === query.credential_id) {
      void cycleQuery.refetch()
    }
  }
  const filter = { ...query, page: null, page_size: null }
  // Detach the inactive view's observers so in-flight requests can be aborted.
  const history = view === "quotas" ? { from: query.from, to: query.to, credential_id: query.credential_id, provider_id: query.provider_id } : null
  const records = useQuery({ queryKey: ["usage-records", view === "records" ? query : null], queryFn: ({ signal }) => usageRecords(query, signal), placeholderData: keepPreviousData, enabled: view === "records" })
  const [summary, credentialQuery, providerQuery, userQuery, keyQuery, cycleQuery] = useQueries({ queries: [
    { queryKey: ["usage-summary", view === "records" ? filter : null], queryFn: ({ signal }) => usageSummary(query, signal), enabled: view === "records" },
    { queryKey: ["credentials"], queryFn: ({ signal }) => fetchCredentials(signal) },
    { queryKey: ["providers"], queryFn: ({ signal }) => fetchProviders(signal) },
    { queryKey: ["users"], queryFn: ({ signal }) => fetchUsers(signal), enabled: view === "records" },
    { queryKey: ["user-keys"], queryFn: ({ signal }) => fetchUserKeys(signal), enabled: view === "records" },
    { queryKey: ["credential-cycles", "history", history], queryFn: ({ signal }) => queryCredentialCycles({ from: query.from, to: query.to, credential_id: query.credential_id, provider_id: query.provider_id, include_history: false, include_estimate: false }, signal), refetchInterval: view === "quotas" ? 60_000 : false, enabled: view === "quotas" },
  ] })
  const totalRequests = summary.data?.requests
  const recordPage = useMemo(() => records.data
    ? { ...records.data, total: records.data.total ?? totalRequests ?? null }
    : { items: EMPTY, total: null, page: 1, page_size: 10, has_more: false }, [records.data, totalRequests])
  const loading = view === "records" ? records.isLoading : cycleQuery.isLoading
  const error = view === "records" ? records.error : cycleQuery.error
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
          loading={loading} error={Boolean(error)}
          onPage={onPage} onPageSize={onPageSize}
          credentials={credentialQuery.data ?? EMPTY} providers={providerQuery.data ?? EMPTY}
          users={userQuery.data ?? EMPTY} keys={keyQuery.data ?? EMPTY}
        >
          <QuotaHistory
            range={{ from: query.from, to: query.to, credential_id: query.credential_id, provider_id: query.provider_id }}
            cycles={cycleQuery.data ?? []}
            providers={providerQuery.data ?? []} credentials={credentialQuery.data ?? []}
            loading={cycleQuery.isLoading || providerQuery.isLoading || credentialQuery.isLoading}
            error={cycleQuery.isError || providerQuery.isError || credentialQuery.isError}
          />
        </UsageExplorer>
    </PageLayout>
  )
}
