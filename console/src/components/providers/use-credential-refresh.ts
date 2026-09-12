import type { CredentialDto } from "@/generated/CredentialDto"
import { useIsMutating, useMutation, useQueryClient } from "@tanstack/react-query"
import { useTranslation } from "react-i18next"
import { toast } from "sonner"
import { ApiError } from "@/api/client"
import { refreshCredential } from "@/api/control"

type RefreshScope = Pick<CredentialDto, "id" | "provider_id" | "version">

export function useCredentialRefresh(credential: CredentialDto) {
  const client = useQueryClient()
  const { t } = useTranslation()
  const mutationKey = ["credential-refresh", credential.id]
  const refreshing = useIsMutating({ mutationKey, exact: true }) > 0
  const mutation = useMutation({
    mutationKey,
    mutationFn: (scope: RefreshScope) => refreshCredential(scope.id, scope.version),
    retry: false,
    onSuccess: async (result, scope) => {
      const quotaFilter = {
        predicate: ({ queryKey }: { queryKey: readonly unknown[] }) =>
          ["credential-quota", "credential-quota-probe"].includes(String(queryKey[0]))
          && queryKey[1] === scope.id
          && typeof queryKey[2] === "number" && queryKey[2] < result.credential_version,
      }
      // Discard reads that began before rotation before publishing its version.
      await Promise.all([
        client.cancelQueries({ queryKey: ["credentials"] }),
        client.cancelQueries(quotaFilter),
      ])
      client.setQueriesData<Array<CredentialDto>>({ queryKey: ["credentials"] }, (current) => current?.map((item) =>
        item.id === scope.id && item.provider_id === scope.provider_id && item.version < result.credential_version
          ? { ...item, version: result.credential_version }
          : item,
      ))
      await Promise.all([
        client.invalidateQueries({ queryKey: ["credentials"] }),
        // The new credential version loads its own snapshot. Do not restart a
        // cancelled probe with an old render's credential while the list loads.
        client.invalidateQueries({ ...quotaFilter, refetchType: "none" }),
        client.invalidateQueries({
          queryKey: ["credential-cycles"],
          predicate: ({ queryKey }) => {
            const filterScope = queryKey[2]
            if (queryKey[1] === "providers") return filterScope === scope.provider_id
            if (filterScope == null || typeof filterScope !== "object") return true
            const filter = filterScope as { credential_id?: number | null; provider_id?: number | null }
            return (filter.credential_id == null || filter.credential_id === scope.id)
              && (filter.provider_id == null || filter.provider_id === scope.provider_id)
          },
        }),
      ])
      const message = t(`providers.credentials.oauthRefresh.outcomes.${result.refresh_token_status}`)
      if (result.refresh_token_status === "not_returned" || result.refresh_token_status === "updated_elsewhere") toast.info(message)
      else toast.success(message)
    },
    onError: async (error) => {
      toast.error(error instanceof ApiError ? error.message : t("providers.credentials.oauthRefresh.error"))
      await client.cancelQueries({ queryKey: ["credentials"] })
      await client.invalidateQueries({ queryKey: ["credentials"] })
    },
  })
  return {
    refreshing,
    refresh: () => {
      // Desktop and mobile actions can both be mounted for the same account.
      // Check the shared mutation cache synchronously to reject duplicate clicks.
      if (!credential.enabled || !credential.refresh_supported || client.isMutating({ mutationKey, exact: true })) return
      mutation.mutate({ id: credential.id, provider_id: credential.provider_id, version: credential.version })
    },
  }
}
