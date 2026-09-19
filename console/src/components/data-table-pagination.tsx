import { ChevronLeftIcon, ChevronRightIcon } from "lucide-react"
import { useId } from "react"
import { useTranslation } from "react-i18next"
import { Button } from "@/components/ui/button"
import { PAGE_SIZES, type PageSize } from "@/components/data-table-state"

export type { PageSize }
import { Field, FieldLabel } from "@/components/ui/field"
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"


type Props = {
  page: number
  pages: number | null
  hasMore?: boolean
  pageSize: PageSize
  pending?: boolean
  onPage: (page: number) => void
  onPageSize: (pageSize: PageSize) => void
}

export function DataTablePagination({ page, pages, hasMore, pageSize, pending = false, onPage, onPageSize }: Props) {
  const { t } = useTranslation()
  const pageSizeId = useId()
  const label = t(pages == null ? "common.dataTable.pageUnknown" : "common.dataTable.page", { page, pages })
  return (
    <nav className="flex flex-wrap items-center justify-between gap-2" aria-label={label}>
      <Field orientation="horizontal" className="w-fit">
        <FieldLabel htmlFor={pageSizeId}>{t("common.dataTable.itemsPerPage")}</FieldLabel>
        <Select value={String(pageSize)} onValueChange={(value) => onPageSize(Number(value) as PageSize)}>
          <SelectTrigger id={pageSizeId} size="sm">
            <SelectValue />
          </SelectTrigger>
          <SelectContent align="start">
            <SelectGroup>
              {PAGE_SIZES.map((size) => <SelectItem key={size} value={String(size)}>{size}</SelectItem>)}
            </SelectGroup>
          </SelectContent>
        </Select>
      </Field>
      <div className="flex items-center gap-2">
        <span className="text-xs text-muted-foreground">{label}</span>
        <Button size="icon-sm" variant="outline" disabled={pending || page <= 1} onClick={() => onPage(page - 1)} aria-label={t("common.dataTable.previous")}>
          <ChevronLeftIcon aria-hidden />
        </Button>
        <Button size="icon-sm" variant="outline" disabled={pending || !(hasMore ?? (pages != null && page < pages))} onClick={() => onPage(page + 1)} aria-label={t("common.dataTable.next")}>
          <ChevronRightIcon aria-hidden />
        </Button>
      </div>
    </nav>
  )
}
