import type { TFunction } from "i18next"
import type { CredentialDto } from "@/generated/CredentialDto"
import type { CredentialQuotaCycleDto } from "@/generated/CredentialQuotaCycleDto"
import type { CycleObservationDto } from "@/generated/CycleObservationDto"
import type { ProviderDto } from "@/generated/ProviderDto"
import { windowName } from "@/lib/quota-window"

export type QuotaMetric = "percent" | "tokens" | "cost"
export type QuotaSeries = { id: string; providerId: string; provider: string; label: string; color: string; cycles: Array<CredentialQuotaCycleDto> }
export type QuotaPoint = { at: number; value: number | null }
export type QuotaRound = { at: number; value: number; range: [number, number]; minimum: number; maximum: number; count: number; cycleId: number; observedAt: number }

function amount(value: string | null | undefined): number | null {
  if (value == null || !value.trim()) return null
  const parsed = Number(value)
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null
}

export function remainingQuota(sample: CycleObservationDto, metric: QuotaMetric): number | null {
  const used = amount(sample.upstream_used)
  const limit = amount(sample.upstream_limit)
  const percent = amount(sample.used_percent) ?? (used != null && limit != null && limit > 0 ? used / limit * 100 : null)
  if (percent == null) return null
  const remaining = Math.max(0, 100 - percent)
  if (metric === "percent") return remaining
  if (sample.estimate?.reason != null) return null
  const total = amount(sample.estimate?.[metric])
  return total == null ? null : total * remaining / 100
}

export function cyclePoints(cycle: CredentialQuotaCycleDto, metric: QuotaMetric): Array<QuotaPoint> {
  return cycle.observations.map((sample) => ({ at: sample.observed_at_ms, value: remainingQuota(sample, metric) }))
    .sort((left, right) => left.at - right.at)
}

export function roundRange(cycle: CredentialQuotaCycleDto, metric: QuotaMetric): QuotaRound | null {
  let minimum = Infinity
  let maximum = -Infinity
  let count = 0
  let latest: QuotaPoint | null = null
  for (const sample of cycle.observations) {
    const value = remainingQuota(sample, metric)
    if (value == null) continue
    minimum = Math.min(minimum, value)
    maximum = Math.max(maximum, value)
    count++
    if (latest == null || sample.observed_at_ms >= latest.at) latest = { at: sample.observed_at_ms, value }
  }
  if (!latest) return null
  const value = latest.value!
  return { at: cycle.accounting_start_ms, value, range: [value - minimum, maximum - value], minimum, maximum, count, cycleId: cycle.id, observedAt: latest.at }
}

// Bound SVG geometry to the chart's display resolution. Each bucket retains its
// endpoints, extrema and a missing-value marker. Range/count statistics always
// use all observations, independently of this display-only reduction.
export function sampleQuotaPoints(points: QuotaPoint[], limit = 1024): QuotaPoint[] {
  if (points.length <= limit) return points
  const width = Math.ceil(points.length / Math.max(1, Math.floor(limit / 5)))
  const result: QuotaPoint[] = []
  for (let start = 0; start < points.length; start += width) {
    const end = Math.min(points.length, start + width)
    const indices = new Set([start, end - 1])
    let min = -1, max = -1, gap = -1
    for (let i = start; i < end; i++) {
      const value = points[i].value
      if (value == null) { if (gap < 0) gap = i; continue }
      if (min < 0 || value < points[min].value!) min = i
      if (max < 0 || value > points[max].value!) max = i
    }
    for (const i of [min, max, gap]) if (i >= 0) indices.add(i)
    for (const i of [...indices].sort((a, b) => a - b)) result.push(points[i])
  }
  return result
}

export function quotaSeries(cycles: Array<CredentialQuotaCycleDto>, credentials: Array<CredentialDto>, providers: Array<ProviderDto>, t: TFunction): Array<QuotaSeries> {
  const credentialById = new Map(credentials.map((credential) => [credential.id, credential]))
  const providerById = new Map(providers.map((provider) => [provider.id, provider]))
  const groups = new Map<string, Array<CredentialQuotaCycleDto>>()
  for (const cycle of cycles) {
    const key = `${cycle.credential_id}:${cycle.window_key}`
    const group = groups.get(key) ?? []
    group.push(cycle)
    groups.set(key, group)
  }
  return Array.from(groups).sort(([left], [right]) => left.localeCompare(right)).map(([id, cycles], index) => {
    const ordered = [...cycles].sort((left, right) => left.accounting_start_ms - right.accounting_start_ms || left.id - right.id)
    const latest = cycles.reduce((latest, cycle) => cycle.last_observed_at > latest.last_observed_at ? cycle : latest)
    const credential = credentialById.get(latest.credential_id)
    const provider = credential ? providerById.get(credential.provider_id) : undefined
    const providerName = provider?.label ?? provider?.name ?? (credential ? `#${credential.provider_id}` : t("usage.quotaHistory.unknownProvider"))
    return {
      id, providerId: credential ? String(credential.provider_id) : "unknown", provider: providerName,
      label: `${providerName} · ${credential?.label ?? `#${latest.credential_id}`} · ${windowName(latest.window_key, t, latest.label)}`,
      color: `var(--${["state-info", "state-healthy", "state-warning", "state-critical", "primary"][index % 5]})`, cycles: ordered,
    }
  })
}
