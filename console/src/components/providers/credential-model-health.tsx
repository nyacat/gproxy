import type { CredentialHealthDto } from "@/generated/CredentialHealthDto"
import type { CredentialModelHealthDto } from "@/generated/CredentialModelHealthDto"
import { useMutation, useQueryClient } from "@tanstack/react-query"
import { useTranslation } from "react-i18next"
import { toast } from "sonner"
import { ApiError } from "@/api/client"
import { resetCredentialHealth } from "@/api/control"
import { StatusBadge } from "@/components/status-badge"
import { Button } from "@/components/ui/button"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { formatInstant } from "@/lib/format"

export function CredentialHealthBadge({ credentialId, health, models, observedAt }: {
  credentialId: number
  health: CredentialHealthDto
  models: Array<CredentialModelHealthDto>
  observedAt?: number | null
}) {
  const { t, i18n } = useTranslation()
  const client = useQueryClient()
  const reset = useMutation({
    mutationFn: (model: string | undefined) => resetCredentialHealth(credentialId, model),
    onSuccess: async () => {
      await client.invalidateQueries({ queryKey: ["credentials"] })
      toast.success(t("providers.credentials.healthReset.success"))
    },
    onError: (error) => toast.error(error instanceof ApiError ? error.message : t("providers.credentials.healthReset.error")),
  })
  const issues = models
    .filter((value) => value.health === "degraded" || value.health === "dead")
    .sort((left, right) => {
      if (left.model === "*") return -1
      if (right.model === "*") return 1
      return left.model.localeCompare(right.model)
    })
  const accountIssue = issues.find((issue) => issue.model === "*")
  const modelIssueCount = issues.filter((issue) => issue.model !== "*").length
  const accountLabel = accountIssue ? t("providers.credentials.modelHealth.accountStatus", { status: t(`common.status.${accountIssue.health}`) }) : null
  const modelLabel = modelIssueCount ? t("providers.credentials.modelHealth.issues", { count: modelIssueCount }) : null
  const summary = health === "disabled" || !issues.length
    ? t(`common.status.${health}`)
    : [accountLabel, modelLabel].filter(Boolean).join(" · ")
  const observed = formatInstant(observedAt ?? null, i18n.language)
  const scopeLabel = (model: string) => model === "*"
    ? t("providers.credentials.modelHealth.account")
    : model || t("providers.credentials.modelHealth.unspecified")
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          className="inline-flex flex-wrap gap-1 rounded-full outline-none focus-visible:ring-2 focus-visible:ring-ring"
          aria-label={`${t("providers.credentials.modelHealth.view")}: ${summary}`}
          onClick={(event) => event.stopPropagation()}
        >
          {health === "disabled" || !issues.length ? <StatusBadge status={health} /> : (
            <>
              {accountIssue ? <StatusBadge status={accountIssue.health} label={accountLabel ?? undefined} /> : null}
              {modelLabel ? <StatusBadge status="degraded" label={modelLabel} /> : null}
            </>
          )}
        </button>
      </PopoverTrigger>
      <PopoverContent
        align="start"
        className="w-96 max-w-[calc(100vw-2rem)]"
        aria-label={t("providers.credentials.healthDetail")}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex flex-col gap-1">
          <p className="font-medium">{t("providers.credentials.healthDetail")}</p>
          {observed ? <p className="text-xs text-muted-foreground">{t("providers.credentials.healthObserved", { time: observed })}</p> : null}
        </div>
        {issues.length ? (
          <ul className="max-h-80 space-y-3 overflow-y-auto">
            {issues.map((issue) => (
              <li key={issue.model} className="flex flex-col gap-1">
                <div className="flex items-start justify-between gap-2">
                  <div className="flex min-w-0 flex-wrap items-center gap-1.5">
                    <span className="break-all font-mono text-xs">{scopeLabel(issue.model)}</span>
                    <StatusBadge status={issue.health} />
                  </div>
                  <Button
                    type="button"
                    variant="outline"
                    size="xs"
                    disabled={reset.isPending}
                    aria-label={t("providers.credentials.healthReset.scope", { scope: scopeLabel(issue.model) })}
                    onClick={() => reset.mutate(issue.model)}
                  >
                    {t("providers.credentials.healthReset.action")}
                  </Button>
                </div>
                <p className="text-xs text-muted-foreground">{t("providers.credentials.healthObserved", { time: formatInstant(issue.observed_at, i18n.language) })}</p>
                {issue.response_status != null || issue.detail ? <p className="break-words font-mono text-xs text-muted-foreground">{[issue.response_status, issue.detail].filter((value) => value != null && value !== "").join(" · ")}</p> : null}
              </li>
            ))}
          </ul>
        ) : <p className="text-xs text-muted-foreground">{t("providers.credentials.modelHealth.none")}</p>}
        <div className="flex flex-col items-start gap-2 border-t pt-2.5">
          <p className="text-xs text-muted-foreground">{t("providers.credentials.healthReset.hint")}</p>
          <Button type="button" variant="outline" size="xs" disabled={reset.isPending || !models.length} onClick={() => reset.mutate(undefined)}>
            {t("providers.credentials.healthReset.all")}
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  )
}
