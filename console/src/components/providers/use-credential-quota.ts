import type { CredentialDto } from "@/generated/CredentialDto"
import type { QuotaProbeResponse } from "@/generated/QuotaProbeResponse"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { credentialQuota, probeCredentialQuota } from "@/api/control"

const freshnessMs = 10 * 60 * 1000
type QuotaScope = Pick<CredentialDto, "id" | "provider_id" | "version">

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
  const storeResult = async (result: QuotaProbeResponse, scope: QuotaScope, signal?: AbortSignal) => {
    const key = ["credential-quota", scope.id, scope.version]
    // An older background snapshot may still be in flight when this probe
    // completes. Cancel that read before publishing the refreshed snapshot.
    await client.cancelQueries({ queryKey: key, exact: true })
    signal?.throwIfAborted()
    client.setQueryData(key, result.snapshot)
    void client.invalidateQueries({
      queryKey: ["credential-cycles"],
      predicate: ({ queryKey }) => {
        const filterScope = queryKey[2]
        if (queryKey[1] === "providers") return filterScope === scope.provider_id
        if (filterScope == null || typeof filterScope !== "object") return true
        const filter = filterScope as { credential_id?: number | null; provider_id?: number | null }
        return (filter.credential_id == null || filter.credential_id === scope.id)
          && (filter.provider_id == null || filter.provider_id === scope.provider_id)
      },
    })
  }
  const probe = useQuery({
    queryKey: probeKey,
    queryFn: async ({ signal }) => {
      const result = await probeCredentialQuota(credential.id, false, true, signal)
      await storeResult(result, credential, signal)
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
    mutationKey: ["credential-quota-manual", credential.id, credential.version],
    mutationFn: (scope: QuotaScope) => probeCredentialQuota(scope.id, true, true),
    // Mutation callbacks may receive new render closures while a request is
    // pending. Bind writes to the identity and version captured when it began.
    onSuccess: async (result, scope) => {
      await storeResult(result, scope)
      client.setQueryData(["credential-quota-probe", scope.id, scope.version], result)
    },
  })
  return {
    snapshot: saved.data,
    quota: probe.data,
    canProbe,
    loading: saved.isPending,
    refreshing: probe.isFetching || manual.isPending,
    error: saved.error ?? manual.error ?? probe.error,
    refresh: () => manual.mutateAsync(credential),
  }
}
