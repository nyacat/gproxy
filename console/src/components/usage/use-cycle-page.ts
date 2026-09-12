import { useState } from "react"
import { hashKey, useQuery } from "@tanstack/react-query"
import { credentialCyclePage, type CyclePageRead } from "@/api/observability"
import type { CredentialCycleCursorDto } from "@/generated/CredentialCycleCursorDto"

export function useCyclePage(range: Omit<CyclePageRead, "cursor" | "limit">) {
  const key = hashKey([range])
  const initial = { key, cursors: [null] as Array<CredentialCycleCursorDto | null>, page: 0 }
  const [position, setPosition] = useState(initial)
  if (position.key !== key) setPosition(initial)
  const current = position.key === key ? position : initial
  const request = { ...range, cursor: current.cursors[current.page], limit: 10 }
  const query = useQuery({
    queryKey: ["credential-cycles", "page", request],
    queryFn: ({ signal }) => credentialCyclePage(request, signal),
    refetchInterval: 60_000,
    gcTime: 0,
  })
  return {
    query,
    page: current.page,
    previous: () => setPosition({ ...current, page: Math.max(0, current.page - 1) }),
    next: () => {
      const cursor = query.data?.next_cursor
      if (cursor) setPosition({ key, cursors: [...current.cursors.slice(0, current.page + 1), cursor], page: current.page + 1 })
    },
  }
}
