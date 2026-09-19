import { useMemo, useState } from "react"
import { ChevronsUpDownIcon } from "lucide-react"
import { defaultFilter } from "cmdk"
import { Button } from "@/components/ui/button"
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { cn } from "@/lib/utils"
import { OptionPages, OPTIONS_PER_PAGE } from "@/components/option-pages"

export type SearchableOption = { value: string; label: string; keywords?: string }

export function SearchableSelect({
  value,
  options,
  placeholder,
  searchPlaceholder,
  emptyLabel,
  ariaLabel,
  id,
  disabled,
  onChange,
}: {
  value: string
  options: Array<SearchableOption>
  placeholder: string
  searchPlaceholder: string
  emptyLabel: string
  ariaLabel: string
  id?: string
  disabled?: boolean
  onChange: (value: string) => void
}) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState("")
  const [page, setPage] = useState(0)
  const matches = useMemo(() => {
    if (!search) return options
    return options.map((option) => ({ option, score: defaultFilter(`${option.label} ${option.keywords ?? ""}`.trim(), search) }))
      .filter(({ score }) => score > 0)
      .sort((a, b) => b.score - a.score)
      .map(({ option }) => option)
  }, [options, search])
  const pages = Math.max(1, Math.ceil(matches.length / OPTIONS_PER_PAGE))
  const currentPage = Math.min(page, pages - 1)
  const visible = matches.slice(currentPage * OPTIONS_PER_PAGE, (currentPage + 1) * OPTIONS_PER_PAGE)
  const selected = options.find((option) => option.value === value)
  return (
    <Popover modal open={open} onOpenChange={(next) => { setOpen(next); if (next) { setSearch(""); setPage(0) } }}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          id={id}
          variant="outline"
          role="combobox"
          aria-label={ariaLabel}
          aria-expanded={open}
          disabled={disabled}
          className="w-full justify-between font-normal"
        >
          <span className={cn("truncate", !selected && "text-muted-foreground")}>
            {selected?.label ?? placeholder}
          </span>
          <ChevronsUpDownIcon data-icon="inline-end" className="opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-(--radix-popover-trigger-width) p-0" align="start">
        <Command shouldFilter={false}>
          <CommandInput placeholder={searchPlaceholder} value={search} onValueChange={(next) => { setSearch(next); setPage(0) }} />
          <CommandList>
            <CommandEmpty>{emptyLabel}</CommandEmpty>
            <CommandGroup>
              {visible.map((option) => (
                <CommandItem
                  key={option.value}
                  value={option.value}
                  data-checked={option.value === value}
                  onSelect={() => {
                    onChange(option.value)
                    setOpen(false)
                  }}
                >
                  <span className="truncate">{option.label}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
        <OptionPages page={currentPage} pages={pages} onPage={setPage} />
      </PopoverContent>
    </Popover>
  )
}
