# skaidb examples

Runnable, self-contained examples of using skaidb — one per language, plus a
separate section for vector search. Each language example does the same
thing (so you can compare them side by side): connect, create a table,
batch-insert with bound parameters, query, update, a primary-key point read,
error handling, delete, and cleanup.

These are standalone walkthroughs; for the driver source itself and
installation instructions see [`../drivers/`](../drivers/).

| Language | Directory | Run | Status |
|----------|-----------|-----|--------|
| Python | [`python/`](python/) | `python3 basic_usage.py [host] [port] [user] [password]` | ✅ verified |
| Node.js / TS | [`nodejs/`](nodejs/) | `node basic_usage.js [host] [port] [user] [password]` | ✅ verified |
| Go | [`go/`](go/) | `cd go && go run . "skaidb://user:pass@host:port/"` | ✅ verified |
| Rust | [`rust/`](rust/) | `cd rust && cargo run --bin basic_usage -- host:port [user] [pass]` | ✅ verified |
| Java | [`java/`](java/) | see [`java/`](java/) for the two-step compile & run | ✅ verified |
| Ruby | [`ruby/`](ruby/) | `ruby basic_usage.rb [host] [port] [user] [password]` | ⚙️ spec-verified |
| PHP | [`php/`](php/) | `php basic_usage.php [host] [port] [user] [password]` | ⚙️ spec-verified |
| C# / .NET | [`dotnet/`](dotnet/) | `dotnet run --project dotnet -- [host] [port] [user] [password]` | ⚙️ spec-verified |

> **verified** = run end-to-end against a live skaidb node in the authoring
> environment. **spec-verified** = written and cross-checked against the
> driver's implementation and the other verified examples, but the language
> runtime wasn't available to execute it directly — see
> [`../drivers/README.md`](../drivers/README.md) for the same distinction
> applied to the drivers themselves.

All examples default to `localhost:7000` with anonymous auth — the shape a
freshly-started `skaidb` (see [`../README.md`](../README.md#run-the-server))
listens on. Pass a host/port/user/password to point at a real cluster.

## Vector search

[`vectors/`](vectors/) is a separate, focused walkthrough of the embedding
workflow — turning text into a vector, storing it, indexing it, and running
nearest-neighbor search — independent of the basic-usage examples above.
