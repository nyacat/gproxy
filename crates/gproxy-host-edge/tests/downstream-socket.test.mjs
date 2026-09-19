import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import test from "node:test"

// Execute the exact JavaScript shipped by wasm-bindgen, including its promise
// queue. A Rust mock cannot exercise a rejected Blob.arrayBuffer() promise.
const rust = await readFile(new URL("../src/websocket/js.rs", import.meta.url), "utf8")
const source = rust.split('#[wasm_bindgen(inline_js = r#"')[1]?.split('"#)]')[0]
assert.ok(source, "wasm-bindgen inline JavaScript is present")
const module = await import(`data:text/javascript,${encodeURIComponent(`${source}\nexport { GproxyDownstreamSocket };`)}`)

function fixture() {
  const listeners = new Map()
  const socket = new module.GproxyDownstreamSocket({
    binaryType: "blob",
    addEventListener(name, listener) { listeners.set(name, listener) },
  })
  return { socket, emit: (name, event) => listeners.get(name)(event) }
}

async function next(socket) {
  const frame = await socket.recv()
  socket.ack()
  return frame
}

test("failed Blob decoding reaches the pump and does not swallow close", async () => {
  const { socket, emit } = fixture()
  emit("message", { data: { arrayBuffer: async () => { throw new Error("Blob read failed") } } })
  emit("close", { code: 1006 })
  await socket.sequence

  assert.deepEqual(await next(socket), ["error", null])
  assert.deepEqual(await next(socket), ["close", 1006])
  assert.equal(await next(socket), null)
})

test("asynchronous Blob reads retain message and close ordering", async () => {
  const { socket, emit } = fixture()
  const { promise, resolve } = Promise.withResolvers()
  emit("message", { data: { arrayBuffer: () => promise } })
  emit("message", { data: "after binary" })
  emit("close", { code: 1000 })
  resolve(new Uint8Array([1, 2, 3]).buffer)
  await socket.sequence

  assert.deepEqual(await next(socket), ["binary", new Uint8Array([1, 2, 3])])
  assert.deepEqual(await next(socket), ["text", "after binary"])
  assert.deepEqual(await next(socket), ["close", 1000])
  assert.equal(await next(socket), null)
})

test("an interrupted Rust receive keeps the same pending frame until acknowledged", async () => {
  const { socket, emit } = fixture()
  const pending = socket.recv()
  assert.equal(socket.recv(), pending)
  emit("message", { data: "first" })
  emit("message", { data: "second" })
  await socket.sequence

  assert.equal(socket.recv(), pending)
  assert.deepEqual(await next(socket), ["text", "first"])
  assert.deepEqual(await next(socket), ["text", "second"])
})
