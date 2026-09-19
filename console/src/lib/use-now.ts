import { useEffect, useState } from "react"

const precisionSeconds = 60

function snapshot() {
  return Math.floor(Date.now() / 1000)
}

export function useNow() {
  // The clock changes only when we notify React. Reading Date.now() from an
  // external-store snapshot can invalidate a concurrent render without a tick.
  const [now, setNow] = useState(snapshot)
  useEffect(() => {
    const update = () => setNow(snapshot())
    const timer = window.setInterval(update, precisionSeconds * 1000)
    window.addEventListener("focus", update)
    return () => {
      window.clearInterval(timer)
      window.removeEventListener("focus", update)
    }
  }, [])
  return now
}
