import { useTranslation } from "react-i18next"
import { Button } from "@/components/ui/button"

export type CyclePagination = { page: number; hasMore: boolean; pending: boolean; previous: () => void; next: () => void }

export function CyclePageNavigation({ page, hasMore, pending, previous, next }: CyclePagination) {
  const { t } = useTranslation()
  const label = t("common.dataTable.pageUnknown", { page: page + 1 })
  return <nav aria-label={label} className="flex items-center justify-between gap-2">
    <Button variant="outline" size="sm" disabled={pending || page === 0} onClick={previous}>{t("common.dataTable.previous")}</Button>
    <span className="text-xs tabular-nums">{label}</span>
    <Button variant="outline" size="sm" disabled={pending || !hasMore} onClick={next}>{t("common.dataTable.next")}</Button>
  </nav>
}
