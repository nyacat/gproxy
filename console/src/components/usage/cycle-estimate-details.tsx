import { useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { useTranslation } from "react-i18next"
import type { CredentialQuotaCycleDto } from "@/generated/CredentialQuotaCycleDto"
import { queryCredentialCycles } from "@/api/observability"
import { Button } from "@/components/ui/button"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { QueryState } from "@/components/query-state"
import { CredentialCycleModels } from "@/components/providers/credential-cycle-models"
import { CycleUsage } from "./cycle-usage"

export function CycleEstimateDetails({ cycle }: { cycle: CredentialQuotaCycleDto }) {
  const { t } = useTranslation()
  const [open, setOpen] = useState(false)
  return <Collapsible open={open} onOpenChange={setOpen}>
    <CollapsibleTrigger asChild><Button variant="ghost" size="sm">{t("usage.cycleUsage.estimateDetails")}</Button></CollapsibleTrigger>
    <CollapsibleContent>{open ? <Estimate cycle={cycle} /> : null}</CollapsibleContent>
  </Collapsible>
}

function Estimate({ cycle }: { cycle: CredentialQuotaCycleDto }) {
  const { t } = useTranslation()
  // Latest summary selection is independent of the cycle's potentially long
  // lifetime; the exact id also prevents reading neighbouring rounds.
  const request = { from: cycle.last_observed_at, to: cycle.last_observed_at + 86_400, cycle_ids: [cycle.id], credential_id: cycle.credential_id, include_estimate: true }
  const query = useQuery({ queryKey: ["credential-cycles", "detail", request, cycle.version], queryFn: ({ signal }) => queryCredentialCycles(request, signal, "quota-details"), refetchInterval: 60_000 })
  const value = query.data?.find((value) => value.id === cycle.id)
  return <QueryState loading={query.isLoading} error={query.error ? t("common.loadError") : ""}>
    {value ? <><CycleUsage cycle={value} /><CredentialCycleModels values={value.models} /></> : <p className="text-xs text-muted-foreground">{t("usage.cycleUsage.localUnavailable")}</p>}
  </QueryState>
}
