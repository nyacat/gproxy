import type { AuditEventDto } from "@/generated/AuditEventDto"
import type { ChannelDto } from "@/generated/ChannelDto"
import type { CredentialQuotaCycleDto } from "@/generated/CredentialQuotaCycleDto"
import type { CredentialCycleReadRequest } from "@/generated/CredentialCycleReadRequest"
import type { CredentialCyclePageRequest } from "@/generated/CredentialCyclePageRequest"
import type { CredentialCyclePageDto } from "@/generated/CredentialCyclePageDto"
import type { QuotaWindowDto } from "@/generated/QuotaWindowDto"
import type { TlsPresetDto } from "@/generated/TlsPresetDto"
import type { UsageQueryDto } from "@/generated/UsageQueryDto"
import type { UsageStatisticsDto } from "@/generated/UsageStatisticsDto"
import type { UsageRecordQueryDto } from "@/generated/UsageRecordQueryDto"
import type { UsageRecordPageDto } from "@/generated/UsageRecordPageDto"
import type { UsageSummaryDto } from "@/generated/UsageSummaryDto"
import type { UsageTrendPointDto } from "@/generated/UsageTrendPointDto"
import type { UsageTrendQueryDto } from "@/generated/UsageTrendQueryDto"
import type { LogDetailDto } from "@/generated/LogDetailDto"
import type { LogPageDto } from "@/generated/LogPageDto"
import type { LogQueryDto } from "@/generated/LogQueryDto"
import { api, json } from "@/api/client"

const queryString = (entries: object) => {
  const query = new URLSearchParams()
  for (const [key, value] of Object.entries(entries)) {
    if (value != null && value !== "") query.set(key, String(value))
  }
  return query.toString()
}

export const channels = () => api<Array<ChannelDto>>("/admin/api/channels")
export const tlsPresets = () => api<Array<TlsPresetDto>>("/admin/api/tls-presets")
export const usage = (value: UsageQueryDto, signal?: AbortSignal) =>
  api<Array<UsageStatisticsDto>>(`/admin/api/usage?${queryString(value)}`, { signal })
export const usageRecords = (value: UsageRecordQueryDto, signal?: AbortSignal) =>
  api<UsageRecordPageDto>(`/admin/api/usage-records?${queryString(value)}`, { signal })
export const usageSummary = (value: UsageRecordQueryDto, signal?: AbortSignal) => {
  const filter = { ...value, page: null, page_size: null }
  return api<UsageSummaryDto>(`/admin/api/usage-summary?${queryString(filter)}`, { signal })
}
export const usageTrend = (value: UsageTrendQueryDto, signal?: AbortSignal) =>
  api<Array<UsageTrendPointDto>>(`/admin/api/usage-trend?${queryString(value)}`, { signal })
export const quotaWindows = (subjectKind?: string, subjectId?: number, signal?: AbortSignal) =>
  api<Array<QuotaWindowDto>>(
    `/admin/api/quota-windows?${queryString({ subject_kind: subjectKind ?? "", subject_id: subjectId ?? null })}`,
    { signal },
  )
export const credentialCycles = (from: number, to: number, credentialId?: number, includeHistory = false, options: { signal?: AbortSignal; providerId?: number; includeEstimate?: boolean } = {}) =>
  api<Array<CredentialQuotaCycleDto>>(
    `/admin/api/credential-cycles?${queryString({ from, to, credential_id: credentialId ?? null, provider_id: options.providerId ?? null, include_history: includeHistory ? "true" : null, include_estimate: options.includeEstimate ?? null })}`,
    { signal: options.signal },
  )
export type CycleRead = Pick<CredentialCycleReadRequest, "from" | "to"> & Partial<CredentialCycleReadRequest>
export type CyclePageRead = Pick<CredentialCyclePageRequest, "from" | "to"> & Partial<CredentialCyclePageRequest>
export const credentialCyclePage = (request: CyclePageRead, signal?: AbortSignal) =>
  api<CredentialCyclePageDto>("/admin/api/credential-cycles/page", { ...json("POST", request), signal })
export const queryCredentialCycles = (request: CycleRead, signal?: AbortSignal, view: "overview" | "providers" | "quota-history" | "quota-details" = "quota-history") =>
  api<Array<CredentialQuotaCycleDto>>("/admin/api/credential-cycles/query", {
    ...json("POST", request), signal,
    headers: { "content-type": "application/json", "x-gproxy-console-view": view },
  })
export const audit = (limit = 100, signal?: AbortSignal) => api<Array<AuditEventDto>>(`/admin/api/audit?limit=${limit}`, { signal })
export const logs = (value: LogQueryDto, signal?: AbortSignal) => api<LogPageDto>(`/admin/api/logs?${queryString(value)}`, { signal })
export const logDetail = (requestId: string, signal?: AbortSignal) => api<LogDetailDto>(`/admin/api/logs/${encodeURIComponent(requestId)}`, { signal })
