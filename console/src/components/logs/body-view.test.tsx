import { fireEvent, render, screen } from "@testing-library/react"
import { expect, it } from "vitest"
import "@/i18n"
import { BodyView } from "./body-view"
import { LOG_SEGMENT_SIZE } from "@/lib/log-content"

it("keeps large log bodies out of the DOM and resets the segment when the body changes", () => {
  const value = "[redacted]".repeat(20_000) + "end-marker"
  const { container, rerender } = render(<BodyView value={value} />)
  expect(container.querySelector("pre")!.textContent!.length).toBeLessThanOrEqual(LOG_SEGMENT_SIZE + 1)
  expect(container.querySelectorAll("mark").length).toBeLessThan(1700)
  const pages = Math.ceil(value.length / LOG_SEGMENT_SIZE)
  for (let page = 1; page < pages; page++) fireEvent.click(screen.getByRole("button", { name: "Next" }))
  expect(container.querySelector("pre")).toHaveTextContent("end-marker")
  expect(screen.getByRole("button", { name: "Next" })).toBeDisabled()
  rerender(<BodyView value="replacement" />)
  expect(container.querySelector("pre")).toHaveTextContent("replacement")
  expect(screen.queryByRole("navigation")).not.toBeInTheDocument()
})
