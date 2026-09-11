import { expect, it } from "vitest"
import { sampleQuotaPoints } from "./quota-history-data"

it("bounds dense geometry while retaining endpoints, extrema and missing samples", () => {
  const points = Array.from({ length: 50_000 }, (_, at) => ({ at, value: at % 17 as number | null }))
  points[24_999].value = 10_000
  points[10_333].value = -10_000
  points[30_127].value = null
  const sampled = sampleQuotaPoints(points)
  expect(sampled.length).toBeLessThanOrEqual(1024)
  expect(sampled[0]).toEqual(points[0])
  expect(sampled.at(-1)).toEqual(points.at(-1))
  expect(sampled).toContain(points[24_999])
  expect(sampled).toContain(points[10_333])
  expect(sampled).toContain(points[30_127])
  expect(sampled.every((point, i) => i === 0 || point.at > sampled[i - 1].at)).toBe(true)
  expect(points).toHaveLength(50_000)
})
