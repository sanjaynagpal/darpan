# Response Write Timeout

a server application written with ktor and embedded netty with default configuration. The server provides a stream of ndjson on /cmd/query  with a final JSON object as COMPLETE to ensure the client gets a definite signal of completion.  A client can send a query request and the response is a stream of Instrument type object that it has to process and save to a  file. The client process is not a fast consumer. There can be a request that can result in close to  million Instrument response. During operation the application gets ClosedByteChannelException and a few seconds later the client gets and exception which is a premature EOF. What could be the root cause?

Confirmed: Ktor's Netty engine default has responseWriteTimeoutSeconds set to 10 seconds. That default is almost certainly the mechanism behind what you're seeing. Here's the causal chain:

## Root cause

**The client can't drain the socket as fast as the server produces ~1M Instrument records → TCP backpressure builds up → Netty's write times out → server force-closes the channel → client sees a truncated stream.**

Step by step:

1. **Slow consumer + fast producer.** Your server is streaming ndjson for a request with ~1M objects. If the client is doing anything non-trivial per line (parsing, writing to disk, especially if it's *synchronous* file I/O or doing it record-by-record without buffering), it reads from its socket receive buffer much slower than the server can write.

2. **TCP flow control kicks in.** As the client's receive buffer fills (because the app isn't draining it), the client's TCP stack advertises a shrinking, eventually zero, receive window. The server's kernel can no longer push bytes onto the wire.

3. **Netty's send buffer/high-water-mark fills.** On the server side, Netty keeps writing (or queuing writes) into the channel. Once the OS socket send buffer is full and TCP window is closed, the `ChannelFuture` for a given `write()` doesn't complete — it just sits pending.

4. **`responseWriteTimeoutSeconds` (default 10s) fires.** Ktor's Netty engine installs a write-timeout handler per response. If a single write doesn't complete within that window (which is very plausible with a slow consumer under backpressure), Ktor/Netty treats it as a failure and closes the channel out from under the still-writing coroutine. That's exactly where your `ClosedByteChannelException` comes from — your handler (or the `ByteWriteChannel` used by `call.respondBytesWriter`/`respondOutputStream`/flow-based streaming) tries to write another ndjson line to a channel that Netty has just forcibly closed.

5. **A few seconds later, client sees premature EOF.** Because the connection is torn down mid-response (using chunked transfer encoding, since you're streaming an unbounded/large body), the client never receives the terminating zero-length chunk, and never gets your `COMPLETE` sentinel object. Whatever HTTP client library you're using (OkHttp/Apache/Ktor client/etc.) detects the socket closing before the chunked body was properly terminated and throws "premature EOF" / "unexpected end of stream." The delay is just: TCP retransmits + the outstanding buffered bytes trickling through + propagation of the RST/FIN before the client's read() call actually fails.

So the "few seconds later" isn't a coincidence — it lines up with the ~10s write timeout plus buffered data still draining.

## Why this specifically shows up at ~1M records but not smaller responses

With small responses everything fits in socket buffers and flushes before the consumer's slowness ever matters. At scale, sustained backpressure over the life of a long streaming response is much more likely to hit a single write that stalls past 10 seconds — especially if the client does something like synchronous per-record disk writes, GC pauses, or a slow downstream (e.g., writing through a buffered writer with small flush batches, or antivirus/disk contention).

## How to confirm

- Check server logs/timestamps: the `ClosedByteChannelException` should occur ~10s after the client's read rate visibly drops (e.g., via `tcpdump`/`ss -i` showing the client's receive window near zero, or Netty debug logs around `WriteTimeoutException`).
- On the client, capture the exact exception type/message — "Premature EOF" from OkHttp specifically means the connection closed before the expected chunk terminator or Content-Length was satisfied, which is consistent with this theory.

## Fixes

- **Increase `responseWriteTimeoutSeconds`** as a stopgap (e.g., to a few minutes) — but this just masks the symptom for a still-slow consumer.
- **Fix the client to actually keep pace**: buffer writes, use async/non-blocking file I/O, batch disk flushes (e.g., wrap in `BufferedOutputStream`/`BufferedWriter` with a decent buffer size, or batch N records before writing).
- **Add server-side pacing/backpressure-aware production**: make sure you're using a suspending write API (`ByteWriteChannel.writeStringUtf8`/`writeFully`) directly tied to the flow that produces Instruments, rather than buffering all million objects into memory and blasting them — suspension there should naturally slow production to match the consumer, but it won't fix a *write timeout*, since the timeout fires regardless of whether the stall is "expected" backpressure or a real stall.
- **Consider chunked responses with periodic keep-alive/heartbeat lines** if there are legitimately long processing gaps, so intermediate writes don't go quiet long enough to look stalled to any intermediary.
- If there's a reverse proxy/load balancer in front of Ktor in production, also check its idle/read timeout settings — but based on what you've described (default embedded Netty, direct client), the Ktor `responseWriteTimeoutSeconds` default of 10s is the most likely single culprit.

what if responseWriteTimeoutSeconds is set as 0 and write is suspendable

Good — that removes the Ktor write-timeout mechanism as the driver, but it also changes the picture in an important way: **now nothing on the server side is going to proactively close a slow-but-alive connection.** If the server is still getting `ClosedByteChannelException`, the channel must be getting closed by something *outside* Ktor's control — either the peer, or the network path between them. The most likely candidates:

**1. The client's own HTTP client has an independent socket/read timeout.**
This is the classic gotcha: people fix the server's write timeout and forget the client library has its own defaults. OkHttp defaults `readTimeout` to 10 seconds — defined as the max inactivity between data packets while reading the response. Apache HttpClient, Java's `HttpClient`, and Ktor's own client engines all have similar (often 10–30s) defaults. If your slow consumer causes the server's outbound TCP window to close for long enough that no bytes cross the wire for one of these intervals, the *client* aborts the connection on its own — sends a RST/close — and only then does the server's next write attempt surface as `ClosedByteChannelException` (peer closed the channel). The "few seconds later" client-side exception you see may actually just be the client's own processing backlog: it already has buffered ndjson lines to work through before it gets back to the socket and notices the stream ended abruptly without the `COMPLETE` marker.

**2. `tcpKeepAlive` is `false` by default in Ktor's Netty engine**, and something on the network path times out idle-looking connections.
With no TCP keepalive, a connection that's throttled down to near-zero throughput for a stretch looks indistinguishable from a dead/idle connection to any stateful middlebox — corporate firewall, NAT gateway, VPN concentrator, cloud load balancer — sitting between client and server. Many of these have idle timeouts in the 60s–5min range and will silently drop connection-tracking state or send a RST without either endpoint's application layer being consulted. Worth checking if there's *anything* in the path (even something you don't think of as "infrastructure," like a corporate VPN client) — "default Ktor config" doesn't mean "no network devices between them."

**3. Client-side resource exhaustion.**
"Not a fast consumer" processing ~1M objects raises the question of whether the client is actually streaming to disk incrementally or buffering objects in memory before writing. If it's accumulating a `List<Instrument>` (or similar) rather than writing per-record/per-batch, you can get GC pressure or an outright OOM on the client. A dying/thrashing JVM can stop servicing its socket (no reads happening at the OS level either) long enough to trip #1 or #2, or the process can die outright, which closes the socket at the OS level — again surfacing on the server as `ClosedByteChannelException` on next write.

**4. A second-order effect of removing the timeout: now a truly stuck client just hangs forever.**
Not the cause of what you're seeing now, but worth flagging: with `responseWriteTimeoutSeconds = 0`, a client that never reads at all no longer gets kicked — the server will hold that coroutine, its underlying resources (DB cursor, buffers, any connection-pool checkout used to produce the Instruments), and an event-loop-attached channel open indefinitely. That's a real resource-leak/DoS risk for a public or semi-trusted endpoint, so I wouldn't leave it at 0 without some other bound (e.g., a total-duration `withTimeout` around the whole response, or an application-level idle-detection heartbeat).

**How to tell which one it is:**
- Check whether the closure is a **RST vs. clean FIN** via `tcpdump`/Wireshark at the moment of failure — a RST from the client's IP strongly implicates #1 (client library aborting) or the client crashing; a mid-stream close with no RST from either real endpoint (traceable) implicates a middlebox.
- Check the client's HTTP client configuration for any read/socket/call timeout that's shorter than your worst-case stall duration, and either disable it (set to 0/infinite) or raise it substantially for this streaming endpoint specifically.
- Watch client-side GC logs / heap usage during the run to rule out #3.
- Enable `tcpKeepAlive = true` on the Ktor Netty config as a cheap mitigation for #2, and tune OS-level keepalive intervals (`net.ipv4.tcp_keepalive_time` etc.) down from their (often very long, e.g. 2 hour) OS defaults if you suspect a middlebox with a shorter idle window.



What positive indicator is available from Netty that it closure was due to response write timeout

Netty gives you an unambiguous, purpose-built signal for this: **`io.netty.handler.timeout.WriteTimeoutException`**.

## Why this is the definitive marker

`WriteTimeoutHandler` (which Ktor's Netty engine installs in the pipeline when `responseWriteTimeoutSeconds > 0`) doesn't close the channel silently. When a pending write hasn't completed within the configured window, it explicitly fires this named exception through the pipeline via `ctx.fireExceptionCaught(WriteTimeoutException.INSTANCE)`. That's categorically different from:

- a peer RST/FIN → surfaces as `IOException: Connection reset by peer` or a plain `ClosedChannelException` with no Netty-timeout type anywhere in the chain
- a middlebox silently dropping the connection → same as above, or nothing at all until the next I/O op fails
- your own app-level cancellation → whatever exception you or a coroutine `Job` cancellation produces

So: **if `WriteTimeoutException` appears anywhere in the cause chain or logs for that connection, you have positive proof it was the write-timeout handler; if it doesn't appear, the closure came from somewhere else** (peer, network, or app-level).

## Where to actually find it

A few wrinkles make this less obvious than it should be:

1. **It's a singleton with no stack trace.** Netty constructs it once via `ThrowableUtil.unknownStackTrace(...)` for performance, so you won't get a call-site stack trace — just confirmation of the type. That's fine; you only need the type to know *which* mechanism fired.

2. **Ktor may catch and rewrap it before your code sees it.** Ktor's own call-handling logic (`NettyApplicationCallHandler` / response pipeline) typically reacts to this by closing the response channel, which is exactly what then surfaces as your `ClosedByteChannelException` when the coroutine tries its next write. So your application-level catch block may only ever see the *secondary* exception, not the original `WriteTimeoutException`. To catch the original:
   - Turn on DEBUG/TRACE logging for `io.netty` (and `io.ktor.server.netty`) and grep the logs around the failure timestamp for `WriteTimeoutException`.
   - Or add a `ChannelDuplexHandler` into the pipeline (if you have hook access via `configureBootstrap`/`channelPipelineConfig` in the Netty engine config) that overrides `exceptionCaught` and logs `cause.javaClass.name` — place it so it sees the exception before Ktor's own handler potentially swallows/converts it.
   - Or inspect `channel.closeFuture()`'s listener — `future.cause()` (if set) will show the originating exception rather than just "channel closed."

3. **Timestamp correlation is a weaker but useful secondary signal.** If you don't manage to capture the exception type directly, the fact that the server-side closure occurs at (or very close to) exactly `responseWriteTimeoutSeconds` after the last successful write flush is itself strong circumstantial evidence — but it's not "positive" in the same sense; a middlebox with a similar idle timeout could coincidentally line up. The exception type is the only fully unambiguous signal.

## Practical recommendation

Add a small diagnostic handler once, rather than relying on log-grepping under pressure:

```kotlin
channelPipelineConfig = {
    addLast(object : ChannelDuplexHandler() {
        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            if (cause is io.netty.handler.timeout.WriteTimeoutException) {
                log.warn("Write timeout fired on channel ${ctx.channel().remoteAddress()}")
            }
            super.exceptionCaught(ctx, cause)
        }
    })
}
```

(Exact hook point depends on your Ktor version's exposed config — some versions expose `channelPipelineConfig`, others require a custom `NettyApplicationEngine` subclass — but the principle is the same: get a handler positioned to observe `exceptionCaught` before Ktor's default handling potentially absorbs it.) That gives you a clean, greppable log line tied to remote address and timestamp every time the write-timeout handler — specifically — is the trigger, letting you definitively rule it in or out versus the peer/network causes from the last answer.

ktor 3.4.1 is in use

Good — I pulled the actual pipeline wiring for the Netty engine (this is essentially unchanged through the 3.4.x line), and it confirms the exact handler name and position, which makes the diagnostic hook precise rather than guesswork.

## Confirmed pipeline layout (Ktor 3.4.1, Netty engine, HTTP/1.1 path)

```
codec → continue → timeout (WriteTimeoutHandler) → http1
```

The handler is literally registered as:

```kotlin
pipe.addAfter("continue", "timeout", WriteTimeoutHandler(responseWriteTimeout))
```

So the name `"timeout"` is stable and addressable — you don't need to guess your way to it.

## How to hook it in 3.4.1

`NettyApplicationEngine.Configuration.channelPipelineConfig: ChannelPipeline.() -> Unit` is still exposed in 3.4.x, and it runs as part of `NettyChannelInitializer`'s setup. Use it to insert a diagnostic handler immediately after `"timeout"`, so it sees `WriteTimeoutException` before Ktor's own `http1`/response-handling logic potentially reacts to it and closes things down:

```kotlin
import io.netty.channel.ChannelDuplexHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.timeout.WriteTimeoutException
import io.ktor.server.netty.*
import io.ktor.server.engine.*

embeddedServer(Netty, configure = {
    responseWriteTimeoutSeconds = 10 // or whatever you've set

    channelPipelineConfig = {
        addAfter("timeout", "writeTimeoutDiagnostics", object : ChannelDuplexHandler() {
            override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
                if (cause is WriteTimeoutException) {
                    log.warn(
                        "Netty WriteTimeoutHandler fired: remote={}, channelId={}",
                        ctx.channel().remoteAddress(),
                        ctx.channel().id()
                    )
                }
                super.exceptionCaught(ctx, cause) // must propagate — don't swallow
            }
        })
    }
}) { /* module */ }.start(true)
```

This gives you a clean, unambiguous log line — `WriteTimeoutException` type match, remote address, channel id, timestamp — every time the write-timeout mechanism (as opposed to peer/network closure) is the actual trigger. Critically, calling `super.exceptionCaught(ctx, cause)` at the end preserves Ktor's normal handling of the failure; you're only tapping the event, not intercepting it.

## One caveat specific to 3.4.1: HTTP/2

If you have `enableHttp2 = true`, this pipeline layout doesn't apply the same way — Netty's HTTP/2 multiplexing uses `Http2MultiplexCodecBuilder`/`Http2StreamChannel`s per-stream, and `NettyHttp2Handler` is installed once per connection rather than the linear HTTP/1.1 chain above (this is also the area flagged as having some shared-state quirks across streams in the 3.5 changelog, worth being aware of if you're near that boundary). If your `/cmd/query` traffic could be negotiated over h2, confirm which protocol is actually in play before relying on the `"timeout"` handler name being present in the exact form shown — worth checking with a quick `channel.pipeline().names()` dump on a live connection if you're unsure.

## Practical use

Since you've already got `responseWriteTimeoutSeconds` in play from earlier in this conversation, this hook directly answers the original question: run with it attached, reproduce the slow-consumer scenario, and check whether `writeTimeoutDiagnostics` logs fire at the moment `ClosedByteChannelException` appears. If it does — confirmed write-timeout cause. If the channel closes with no such log line, you're back to the peer/network causes we covered.
