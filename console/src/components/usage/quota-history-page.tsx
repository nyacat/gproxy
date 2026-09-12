import { useState } from "react"
import { hashKey } from "@tanstack/react-query"
import { useTranslation } from "react-i18next"
import type { CyclePageRead } from "@/api/observability"
import type { CredentialDto } from "@/generated/CredentialDto"
import type { ProviderDto } from "@/generated/ProviderDto"
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { QuotaHistory } from "./quota-history"
import { useCyclePage } from "./use-cycle-page"

export function QuotaHistoryPage({ range, providers, credentials, loading, error }: {
  range: CyclePageRead; providers: ProviderDto[]; credentials: CredentialDto[]; loading: boolean; error: boolean
}) {
  const { t } = useTranslation()
  const [windowKey, setWindowKey] = useState("")
  const page = useCyclePage({ ...range, window_key: windowKey.trim() || null })
  return <div className="grid gap-4">
    <Field>
      <FieldLabel htmlFor="quota-history-window">{t("usage.quotaHistory.windowKey")}</FieldLabel>
      <Input id="quota-history-window" value={windowKey} onChange={(event) => setWindowKey(event.target.value)} placeholder={t("usage.filters.all")} />
      <FieldDescription>{t("usage.quotaHistory.windowFilterHint")}</FieldDescription>
    </Field>
    <QuotaHistory selectionKey={hashKey([range, windowKey, page.page])} cycles={page.query.data?.items ?? []} range={range} providers={providers} credentials={credentials}
      loading={loading || page.query.isLoading} error={error || page.query.isError}
      pagination={{ page: page.page, hasMore: page.query.data?.next_cursor != null, pending: page.query.isFetching, previous: page.previous, next: page.next }} />
  </div>
}
