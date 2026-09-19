import { memo, useMemo, useState, type ReactNode } from "react"
import { useTranslation } from "react-i18next"
import { formattedLogContent, logSegment, LOG_SEGMENT_SIZE } from "@/lib/log-content"
import { Button } from "@/components/ui/button"

const marker = /\[redacted\]/gi

function highlighted(value: string): Array<ReactNode> {
  return value.split(marker).flatMap((part, index, values) => index + 1 < values.length
    ? [part, <mark key={index} className="rounded bg-state-warning/20 px-1 font-semibold text-state-warning">[redacted]</mark>]
    : [part])
}

export const BodyView = memo(function BodyView({ value }: { value: string | null }) {
  const { t } = useTranslation()
  const display = useMemo(() => value == null ? null : formattedLogContent(value), [value])
  const [position, setPosition] = useState({ value, page: 0 })
  if (position.value !== value) setPosition({ value, page: 0 })
  const pages = Math.max(1, Math.ceil((display?.length ?? 0) / LOG_SEGMENT_SIZE))
  const page = position.value === value ? Math.min(position.page, pages - 1) : 0
  const content = useMemo(() => display == null ? null : highlighted(logSegment(display, page)), [display, page])
  function download() {
    if (value == null) return
    const url = URL.createObjectURL(new Blob([value], { type: "text/plain;charset=utf-8" }))
    const link = document.createElement("a")
    link.href = url
    link.download = "gproxy-log.txt"
    link.click()
    // Let the browser consume the URL before releasing it.
    window.setTimeout(() => URL.revokeObjectURL(url), 0)
  }
  return (
    <div className="flex min-w-0 flex-col gap-2">
    <pre className="machine-text min-h-20 max-h-72 overflow-auto whitespace-pre-wrap break-words rounded-lg bg-muted p-3 text-xs leading-relaxed">
      {content ?? <span className="text-muted-foreground">{t("logs.detail.notCaptured")}</span>}
    </pre>
    {pages > 1 ? <>
      <p className="text-xs text-muted-foreground">{t("logs.detail.segmentHint")}</p>
      <nav className="flex flex-wrap items-center gap-2" aria-label={t("logs.detail.segments")}>
        <Button size="sm" variant="outline" disabled={page === 0} onClick={() => setPosition({ value, page: page - 1 })}>{t("common.dataTable.previous")}</Button>
        <span className="text-xs tabular-nums">{t("logs.detail.segment", { page: page + 1, pages })}</span>
        <Button size="sm" variant="outline" disabled={page + 1 >= pages} onClick={() => setPosition({ value, page: page + 1 })}>{t("common.dataTable.next")}</Button>
        <Button size="sm" variant="ghost" onClick={download}>{t("logs.detail.download")}</Button>
      </nav>
    </> : null}
    </div>
  )
})
