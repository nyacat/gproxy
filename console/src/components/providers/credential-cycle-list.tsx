import type { CredentialQuotaCycleDto } from "@/generated/CredentialQuotaCycleDto"
import type { QuotaProbeWindowDto } from "@/generated/QuotaProbeWindowDto"
import { useMemo, useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { queryCredentialCycles } from "@/api/observability"
import { CycleEstimateDetails } from "@/components/usage/cycle-estimate-details"
import { QueryState } from "@/components/query-state"
import { OptionPages } from "@/components/option-pages"
import { useTranslation } from "react-i18next"
import { Meter } from "@/components/meter"
import { Button } from "@/components/ui/button"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { formatInstant, formatNumber, formatPercent } from "@/lib/format"
import { windowName } from "@/lib/quota-window"

function CycleTile({ cycle, history }: { cycle: QuotaProbeWindowDto; history?: CredentialQuotaCycleDto }) {
  const { t, i18n } = useTranslation()
  const used = cycle.upstream_used
  const nativeLimit = cycle.upstream_limit
  const limit = Number(nativeLimit)
  const percent = cycle.used_percent != null ? Number(cycle.used_percent)
    : used != null && limit > 0 ? Number(used) / limit * 100 : null
  const value = percent != null ? formatPercent(Math.round(percent) / 100, i18n.language)
    : used != null ? `${formatNumber(Number(used), i18n.language)}${nativeLimit != null ? ` / ${formatNumber(limit, i18n.language)}` : ""}` : "—"
  return <div className="grid min-w-0 gap-2 rounded-lg border bg-card px-3 py-2.5">
    <div className="flex flex-wrap items-baseline justify-between gap-3"><span className="text-sm font-medium">{windowName(cycle.window_key, t, cycle.label)}</span><span className="text-sm font-semibold tabular-nums">{value}</span></div>
    {percent != null ? <Meter percent={percent} /> : null}
    {percent != null && used != null && nativeLimit != null ? <p className="text-xs text-muted-foreground tabular-nums">{formatNumber(Number(used), i18n.language)} / {formatNumber(limit, i18n.language)} {cycle.unit}</p> : null}
    {history?.period_start != null ? <p className="text-xs text-muted-foreground">{t("usage.cycleUsage.starts", { value: formatInstant(history.period_start, i18n.language) })}</p> : null}
    {cycle.period_end != null ? <p className="text-xs text-muted-foreground">{t("window.resets", { value: formatInstant(cycle.period_end, i18n.language) })}</p> : null}
    {history ? <>
      <p className="text-xs text-muted-foreground">{t("usage.cycleUsage.observed", { value: formatInstant(history.last_observed_at, i18n.language) })}</p>
      {history.local_boundary && history.accounting_start_ms != null ? <p className="text-xs text-muted-foreground">{t("usage.cycleUsage.localBoundary", { value: formatInstant(history.accounting_start_ms / 1000, i18n.language) })}</p> : null}
      {history.status === "closed" ? <p className="text-xs text-muted-foreground">{t("common.status.closed")} · {history.close_reason ? t(`window.closeReason.${history.close_reason}`) : ""}</p> : null}
      <CycleEstimateDetails cycle={history} />
    </> : null}
  </div>
}

type Props = {
  credentialId?: number
  cycles: Array<CredentialQuotaCycleDto>
  windows?: Array<QuotaProbeWindowDto>
  loading: boolean
  error: boolean
  localError?: boolean
}

export function CredentialCycleList({ cycles, windows, loading, error, localError = false, credentialId }: Props) {
  const { t } = useTranslation()
  const groups = useMemo(() => {
    const grouped = new Map<string, Array<CredentialQuotaCycleDto>>()
    for (const cycle of cycles) {
      const values = grouped.get(cycle.window_key) ?? []
      values.push(cycle)
      grouped.set(cycle.window_key, values)
    }
    for (const values of grouped.values()) values.sort((left, right) => right.id - left.id)
    return grouped
  }, [cycles])
  const keys = [...new Set([...groups.keys(), ...(windows ?? []).map((window) => window.window_key)])].sort()
  if (loading && !keys.length) return <p className="text-sm text-muted-foreground">{t("common.loading")}</p>
  if (error && !keys.length) return <p className="text-sm text-destructive">{t("common.errors.load")}</p>
  return <div className="grid min-w-0 gap-3">
    {!keys.length ? <p className="text-sm text-muted-foreground">{t(windows ? "providers.credentials.quota.empty" : "providers.credentials.noQuotaCycle")}</p> : null}
    {localError ? <p role="alert" className="text-sm text-muted-foreground">{t("usage.cycleUsage.localUnavailable")}</p> : null}
    {keys.map((key) => {
      const history = groups.get(key) ?? []
      const observed = windows?.find((window) => window.window_key === key)
      const latest = history[0]
      const cycle = windows === undefined ? latest : observed
      const matched = observed
        ? history.find((past) => past.status === "open" && observed.period_end != null && past.period_end === observed.period_end)
        : windows === undefined ? latest : undefined
      const currentHistory = localError ? undefined : matched
      const previous = history.filter((past) => past.id !== matched?.id)
      return <div key={key} className="grid gap-2">
        {cycle ? <CycleTile cycle={cycle} history={currentHistory} /> : null}
        {previous.length ? <Collapsible><CollapsibleTrigger asChild><Button variant="ghost" size="sm">{cycle
          ? t("usage.cycleUsage.history", { count: previous.length })
          : t("upstreamQuota.recordedHistory", { window: windowName(key, t, latest?.label), count: previous.length })}</Button></CollapsibleTrigger><CollapsibleContent className="grid max-h-96 gap-2 overflow-y-auto pt-2"><HistoryTiles cycles={previous} /></CollapsibleContent></Collapsible> : null}
      </div>
    })}
    {credentialId != null ? <PreviousCycles credentialId={credentialId} currentIds={cycles.map((cycle) => cycle.id)} /> : null}
  </div>
}

function HistoryTiles({ cycles }: { cycles: CredentialQuotaCycleDto[] }) {
  const [page, setPage] = useState(0)
  const pages = Math.max(1, Math.ceil(cycles.length / 10))
  const currentPage = Math.min(page, pages - 1)
  return <>
    {cycles.slice(currentPage * 10, (currentPage + 1) * 10).map((cycle) => <CycleTile key={cycle.id} cycle={cycle} history={cycle} />)}
    <OptionPages page={currentPage} pages={pages} onPage={setPage} />
  </>
}

function PreviousCycles({ credentialId, currentIds }: { credentialId: number; currentIds: number[] }) {
  const { t } = useTranslation()
  const [open, setOpen] = useState(false)
  return <Collapsible open={open} onOpenChange={setOpen}>
    <CollapsibleTrigger asChild><Button variant="ghost" size="sm">{t("usage.cycleUsage.loadHistory")}</Button></CollapsibleTrigger>
    <CollapsibleContent>{open ? <PreviousCycleData credentialId={credentialId} currentIds={currentIds} /> : null}</CollapsibleContent>
  </Collapsible>
}

function PreviousCycleData({ credentialId, currentIds }: { credentialId: number; currentIds: number[] }) {
  const { t } = useTranslation()
  const [to] = useState(() => Math.floor(Date.now() / 1000) + 1)
  const request = { from: to - 366 * 86400, to, credential_id: credentialId, include_history: false, include_estimate: false }
  const query = useQuery({ queryKey: ["credential-cycles", "previous", request], queryFn: ({ signal }) => queryCredentialCycles(request, signal, "quota-details"), refetchInterval: 60_000 })
  const previous = (query.data ?? []).filter((cycle) => !currentIds.includes(cycle.id))
  return <QueryState loading={query.isLoading} error={query.error ? t("common.loadError") : ""}>
    {previous.length ? <HistoryTiles cycles={previous} /> : <p className="text-sm text-muted-foreground">{t("providers.credentials.noQuotaCycle")}</p>}
  </QueryState>
}
