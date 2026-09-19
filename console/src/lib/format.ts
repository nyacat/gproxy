// Reuse ICU formatters across table cells, charts and refreshes. Constructing a
// formatter for each value is substantially more expensive than formatting it.
const numbers = new Map<string, Intl.NumberFormat>()
const dates = new Map<string, Intl.DateTimeFormat>()
const cacheSize = 64

function numberFormat(locale: string, options: Intl.NumberFormatOptions = {}) {
  const key = `${locale}:${JSON.stringify(options)}`
  let formatter = numbers.get(key)
  if (!formatter) {
    formatter = new Intl.NumberFormat(locale, options)
    if (numbers.size >= cacheSize) numbers.delete(numbers.keys().next().value!)
    numbers.set(key, formatter)
  }
  return formatter
}

export function dateFormat(locale: string, options: Intl.DateTimeFormatOptions) {
  const key = `${locale}:${JSON.stringify(options)}`
  let formatter = dates.get(key)
  if (!formatter) {
    formatter = new Intl.DateTimeFormat(locale, options)
    if (dates.size >= cacheSize) dates.delete(dates.keys().next().value!)
    dates.set(key, formatter)
  }
  return formatter
}

export function formatCost(value: string | number, locale: string) {
  const amount = typeof value === "number" ? value : Number(value)
  return numberFormat(locale, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: amount < 0.01 ? 4 : 2,
    maximumFractionDigits: amount < 0.01 ? 6 : 2,
  }).format(Number.isFinite(amount) ? amount : 0)
}

export function formatCount(value: number, locale: string) {
  return numberFormat(locale, { notation: value >= 100_000 ? "compact" : "standard" }).format(value)
}

export function formatNumber(value: number, locale: string) {
  return numberFormat(locale, { maximumFractionDigits: 2 }).format(value)
}

export function formatTokensPerSecond(outputTokens: number, latencyMs: number, locale: string) {
  return latencyMs > 0 ? formatNumber(outputTokens * 1000 / latencyMs, locale) : "—"
}

export function formatByteSize(value: number, locale: string) {
  const units = ["B", "KiB", "MiB", "GiB"]
  let amount = Math.max(0, value)
  let unit = 0
  while (amount >= 1024 && unit < units.length - 1) {
    amount /= 1024
    unit += 1
  }
  return `${formatNumber(amount, locale)} ${units[unit]}`
}

export function formatPercent(value: number, locale: string) {
  return numberFormat(locale, { style: "percent", maximumFractionDigits: 1 }).format(value)
}

export function formatInstant(value: number | null, locale: string) {
  if (value == null) return null
  return dateFormat(locale, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value * 1000))
}

export function formatDuration(seconds: number, locale: string) {
  const minutes = Math.max(1, Math.round(seconds / 60))
  if (minutes < 60) return numberFormat(locale, { style: "unit", unit: "minute" }).format(minutes)
  const hours = Math.round(minutes / 60)
  if (hours < 48) return numberFormat(locale, { style: "unit", unit: "hour" }).format(hours)
  return numberFormat(locale, { style: "unit", unit: "day" }).format(Math.round(hours / 24))
}
