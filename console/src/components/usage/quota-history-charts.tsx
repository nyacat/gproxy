import { useQuery } from "@tanstack/react-query"
import { useTranslation } from "react-i18next"
import { queryCredentialCycles, type CycleRead } from "@/api/observability"
import { QueryState } from "@/components/query-state"
import { QuotaHistoryChart } from "./quota-history-chart"
import type { QuotaMetric, QuotaSeries } from "./quota-history-data"

export function QuotaHistoryCharts({ series, metric, range }: { series: QuotaSeries[]; metric: QuotaMetric; range: CycleRead }) {
  const { t } = useTranslation()
  const cycles = series.flatMap((value) => value.cycles).sort((a, b) => a.id - b.id)
  const request = { ...range, cycle_ids: cycles.map((cycle) => cycle.id), include_history: true, include_estimate: metric !== "percent" }
  const query = useQuery({
    queryKey: ["credential-cycles", "estimates", request, cycles.map((cycle) => [cycle.id, cycle.version])],
    queryFn: ({ signal }) => queryCredentialCycles(request, signal),
    enabled: cycles.length > 0,
    refetchInterval: 60_000,
    gcTime: 0,
  })
  const byId = new Map(query.data?.map((cycle) => [cycle.id, cycle]))
  const estimated = series.map((value) => ({ ...value, cycles: value.cycles.map((cycle) => byId.get(cycle.id) ?? { ...cycle, observations: [] }) }))
  return <QueryState loading={cycles.length > 0 && query.isLoading} error={query.error ? t("common.loadError") : ""}>
    <QuotaHistoryChart series={estimated} metric={metric} mode="within" />
    <QuotaHistoryChart series={estimated} metric={metric} mode="across" />
  </QueryState>
}
