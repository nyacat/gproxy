import { fireEvent, render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { expect, it, vi } from "vitest"
import "@/i18n"
import { SearchableSelect } from "./searchable-select"

it("searches the entire option set while mounting one page, and distinguishes duplicate labels", async () => {
  const user = userEvent.setup()
  const onChange = vi.fn()
  const options = Array.from({ length: 10_000 }, (_, i) => ({ value: String(i), label: i < 2 ? "Duplicate" : `Option ${i}` }))
  render(<SearchableSelect value="" options={options} placeholder="Pick" searchPlaceholder="Search options" emptyLabel="Empty" ariaLabel="Large options" onChange={onChange} />)
  await user.click(screen.getByRole("combobox", { name: "Large options" }))
  expect(screen.getAllByRole("option")).toHaveLength(100)
  fireEvent.change(screen.getByPlaceholderText("Search options"), { target: { value: "Opn9999" } })
  expect(await screen.findByRole("option", { name: "Option 9999" })).toBeInTheDocument()
  await user.click(screen.getByRole("option", { name: "Option 9999" }))
  expect(onChange).toHaveBeenLastCalledWith("9999")
  await user.click(screen.getByRole("combobox", { name: "Large options" }))
  fireEvent.change(screen.getByPlaceholderText("Search options"), { target: { value: "Duplicate" } })
  await user.click(screen.getAllByRole("option", { name: "Duplicate" })[1])
  expect(onChange).toHaveBeenLastCalledWith("1")
})
