import { keepPreviousData, useQueries, useQuery } from "@tanstack/react-query"
import { useTranslation } from "react-i18next"
import { credentials, providers } from "@/api/control"
import { queryCredentialCycles, quotaWindows, usage, usageTrend } from "@/api/observability"
import { OverviewDashboard } from "@/components/overview/overview-dashboard"
import { PageLayout } from "@/components/page-layout"
import { QueryState } from "@/components/query-state"
import { useNow } from "@/lib/use-now"

const refreshInterval = 60_000

export function OverviewPage() {
  const { t } = useTranslation()
  const now = useNow()
  const trendTo = now - now % 3_600 + 3_600
  const trendFrom = trendTo - 7 * 86_400
  const trendQuery = useQuery({ queryKey: ["usage-trend", trendFrom, trendTo], queryFn: ({ signal }) => usageTrend({ from: trendFrom, to: trendTo }, signal), refetchInterval: refreshInterval, placeholderData: keepPreviousData })
  const [providerQuery, credentialQuery, usageQuery, quotaQuery, cycleQuery] = useQueries({ queries: [
    { queryKey: ["providers"], queryFn: ({ signal }) => providers(signal), refetchInterval: refreshInterval },
    { queryKey: ["credentials"], queryFn: ({ signal }) => credentials(signal), refetchInterval: refreshInterval },
    { queryKey: ["usage", "provider", "overview"], queryFn: ({ signal }) => {
      const to = Math.floor(Date.now() / 1000)
      return usage({ from: to - 86_400, to, group_by: "provider", user_key_id: null, user_id: null, provider_id: null, credential_id: null, model: null }, signal)
    }, refetchInterval: refreshInterval },
    { queryKey: ["quota-windows"], queryFn: ({ signal }) => quotaWindows(undefined, undefined, signal), refetchInterval: refreshInterval },
    { queryKey: ["credential-cycles", "overview"], queryFn: ({ signal }) => {
      const to = Math.floor(Date.now() / 1000)
      return queryCredentialCycles({ from: to - 604_800, to, current_only: true, include_estimate: false }, signal, "overview")
    }, refetchInterval: refreshInterval },
  ] })
  const queries = [providerQuery, credentialQuery, usageQuery, quotaQuery, cycleQuery]
  const error = queries.find((query) => query.error)?.error
  return (
    <PageLayout title={t("nav.overview")} description={t("usage.overviewDescription")}>
      <QueryState loading={queries.some((query) => query.isLoading)} error={error ? t("common.loadError") : ""}>
        <OverviewDashboard providers={providerQuery.data ?? []} credentials={credentialQuery.data ?? []} usage={usageQuery.data ?? []} quotas={quotaQuery.data ?? []} cycles={cycleQuery.data ?? []} trend={trendQuery.data ?? []} trendFrom={trendFrom} trendTo={trendTo} trendLoading={trendQuery.isLoading} trendError={trendQuery.isError} />
      </QueryState>
    </PageLayout>
  )
}
