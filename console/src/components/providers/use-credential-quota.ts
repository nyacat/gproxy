import type { CredentialDto } from "@/generated/CredentialDto"
import type { QuotaProbeResponse } from "@/generated/QuotaProbeResponse"
import type { QuotaSnapshot } from "@/generated/QuotaSnapshot"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { credentialQuota, probeCredentialQuota } from "@/api/control"

const freshnessMs = 10 * 60 * 1000
type QuotaScope = Pick<CredentialDto, "id" | "provider_id" | "version">

const observedAt = (snapshot: QuotaSnapshot | undefined) => Math.max(0,
  ...snapshot?.sources.flatMap((source) => [source.attempted_at_ms ?? 0, source.observed_at_ms ?? 0]) ?? [],
  ...snapshot?.entries.map((entry) => entry.observed_at_ms) ?? [],
)

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
    const key = ["credential-quota", scope.id, result.credential_version]
    const resultKey = ["credential-quota-probe", scope.id, result.credential_version]
    const preferCurrentSnapshot = () => {
      const current = client.getQueryData<QuotaSnapshot>(key)
      if (!current) return false
      if (observedAt(current) > observedAt(result.snapshot)) return true
      // Match the store's ordering: a successful observation wins an error
      // for the same source and attempt, even when timestamps are identical.
      return current.sources.some((source) => source.error === null
        && source.observed_at_ms !== null && source.attempted_at_ms !== null
        && result.snapshot.sources.some((incoming) => incoming.capability.id === source.capability.id
          && incoming.attempted_at_ms === source.attempted_at_ms && incoming.error !== null))
    }
    // An older background snapshot may still be in flight when this probe
    // completes. Cancel that read before publishing the refreshed snapshot.
    signal?.throwIfAborted()
    await Promise.all([
      !preferCurrentSnapshot() && client.cancelQueries({ queryKey: key, exact: true }),
      result.credential_version > scope.version && client.cancelQueries({ queryKey: ["credentials"] }),
    ])
    signal?.throwIfAborted()
    let published = client.getQueryData<QuotaProbeResponse | null>(resultKey) ?? null
    if (!preferCurrentSnapshot()) {
      client.setQueryData(key, result.snapshot)
      published = client.setQueryData<QuotaProbeResponse>(resultKey, result) ?? null
    }
    if (result.credential_version > scope.version) {
      client.setQueriesData<Array<CredentialDto>>({ queryKey: ["credentials"] }, (current) => current?.map((item) =>
        item.id === scope.id && item.provider_id === scope.provider_id && item.version < result.credential_version
          ? { ...item, version: result.credential_version }
          : item,
      ))
      void client.invalidateQueries({ queryKey: ["credentials"] })
    }
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
    return published
  }
  const probe = useQuery({
    queryKey: probeKey,
    queryFn: async ({ signal }) => {
      const result = await probeCredentialQuota(credential.id, false, true, signal)
      const published = await storeResult(result, credential, signal)
      // OAuth may rotate during the probe. The result belongs to its actual
      // version; a null marker keeps it out of this query's previous-version key.
      return result.credential_version === credential.version ? published : null
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
    // pending. Keep its original account/provider; the response owns the version.
    onSuccess: async (result, scope) => {
      await storeResult(result, scope)
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
