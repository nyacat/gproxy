import type { CredentialDto } from "@/generated/CredentialDto"
import type { QuotaProbeResponse } from "@/generated/QuotaProbeResponse"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { credentialQuota, probeCredentialQuota } from "@/api/control"

const freshnessMs = 10 * 60 * 1000

export function useCredentialQuota(credential: CredentialDto) {
  const client = useQueryClient()
  const snapshotKey = ["credential-quota", credential.id, credential.version]
  const probeKey = ["credential-quota-probe", credential.id, credential.version]
  const saved = useQuery({
    queryKey: snapshotKey,
    queryFn: ({ signal }) => credentialQuota(credential.id, signal),
    retry: false,
    staleTime: 30_000,
  })
  const sources = saved.data?.sources ?? []
  const canProbe = sources.some(({ capability }) => capability.mode === "probe" && capability.support === "ready")
  const storeResult = (result: QuotaProbeResponse) => {
    client.setQueryData(snapshotKey, result.snapshot)
    void client.invalidateQueries({
      queryKey: ["credential-cycles"],
      predicate: ({ queryKey }) => {
        const scope = queryKey[2]
        if (queryKey[1] === "providers") return scope === credential.provider_id
        if (scope == null || typeof scope !== "object") return true
        const filter = scope as { credential_id?: number | null; provider_id?: number | null }
        return (filter.credential_id == null || filter.credential_id === credential.id)
          && (filter.provider_id == null || filter.provider_id === credential.provider_id)
      },
    })
  }
  const probe = useQuery({
    queryKey: probeKey,
    queryFn: async ({ signal }) => {
      const result = await probeCredentialQuota(credential.id, false, true, signal)
      storeResult(result)
      return result
    },
    enabled: () => saved.isSuccess && sources.some(({ capability, attempted_at_ms, observed_at_ms }) =>
      capability.mode === "probe" && capability.support === "ready" && capability.automatic
      && Date.now() - Math.max(attempted_at_ms ?? 0, observed_at_ms ?? 0) >= freshnessMs,
    ),
    retry: false,
    staleTime: freshnessMs,
    gcTime: Infinity,
  })
  const manual = useMutation({
    mutationFn: () => probeCredentialQuota(credential.id, true, true),
    onSuccess: (result) => {
      storeResult(result)
      client.setQueryData(probeKey, result)
    },
  })
  return {
    snapshot: saved.data,
    quota: probe.data,
    canProbe,
    loading: saved.isPending,
    refreshing: probe.isFetching || manual.isPending,
    error: saved.error ?? manual.error ?? probe.error,
    refresh: manual.mutateAsync,
  }
}
