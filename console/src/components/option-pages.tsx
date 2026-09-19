import { useTranslation } from "react-i18next"
import { Button } from "@/components/ui/button"

// Paginate the DOM, not the search space. Search and select-all operations still
// work on every matching option, including those outside the displayed page.
export const OPTIONS_PER_PAGE = 100

export function OptionPages({ page, pages, onPage }: { page: number; pages: number; onPage: (page: number) => void }) {
  const { t } = useTranslation()
  if (pages <= 1) return null
  return <nav className="flex items-center justify-between gap-2 border-t p-1" aria-label={t("common.dataTable.page", { page: page + 1, pages })}>
    <Button type="button" variant="ghost" size="sm" disabled={page === 0} onClick={() => onPage(page - 1)}>{t("common.dataTable.previous")}</Button>
    <span className="text-xs tabular-nums">{t("common.dataTable.page", { page: page + 1, pages })}</span>
    <Button type="button" variant="ghost" size="sm" disabled={page + 1 >= pages} onClick={() => onPage(page + 1)}>{t("common.dataTable.next")}</Button>
  </nav>
}
