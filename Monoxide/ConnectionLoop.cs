using Glyph11;
using Glyph11.Parser;
using Glyph11.Parser.UltraHardened;
using Glyph11.Validation;

using ioxide;

namespace Monoxide;

/// <summary>
/// The per-connection hot loop. Send-first keep-alive: flush whatever the previous read produced,
/// then block on the next recv, copy the io_uring slices into the connection's carry buffer, parse
/// every complete (possibly pipelined) request out of it with Glyph11, route + run each handler into
/// the response buffer, and loop. No Stream, no Pipe — bytes go rings → carry → handler → slab.
/// </summary>
internal static class ConnectionLoop
{
    // One context pool per reactor. ioxide resumes IValueTaskSource continuations inline on the
    // reactor thread, so each reactor only ever touches its own [ThreadStatic] stack — no locks.
    [ThreadStatic] private static Stack<MonoxideContext>? _pool;

    private static MonoxideContext Rent()
        => (_pool ??= new Stack<MonoxideContext>()).TryPop(out MonoxideContext? c) ? c : new MonoxideContext();

    private static void Return(MonoxideContext ctx) => (_pool ??= new Stack<MonoxideContext>()).Push(ctx);

    public static async Task HandleAsync(Reactor reactor, Connection conn, FrozenRouter router, int slab, ParserLimits limits)
    {
        MonoxideContext ctx = Rent();
        ctx.ResetForConnection();
        int carryLen = 0;

        try
        {
            while (true)
            {
                // Send-first: drain the framed response into the write slab (≤ slab per submit) before
                // parking on the next read. A read-first loop would stall a request that arrived bundled
                // with a prior flight (e.g. behind a TLS handshake).
                int sent = 0;
                while (sent < ctx.OutLen)
                {
                    int chunk = Math.Min(ctx.OutLen - sent, slab);
                    conn.Write(ctx.Out.AsSpan(sent, chunk));
                    await conn.FlushAsync();
                    sent += chunk;
                }
                ctx.OutLen = 0;

                if (ctx.WantClose)
                    return;

                RecvSnapshot snap = await conn.ReadAsync();
                carryLen = Drain(conn, snap, ctx, carryLen);

                bool closed = snap.IsClosed;
                if (!closed)
                    conn.ResetRead();

                // Serve every complete request currently in the carry (handles pipelining + fragmentation).
                carryLen = await ParseAll(ctx, router, carryLen, limits);

                if (closed)
                    ctx.WantClose = true;
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[r{reactor.Id}] monoxide handler crash fd={conn.ClientFd}: {ex}");
        }
        finally
        {
            conn.DecRef();
            Return(ctx);
        }
    }

    // Copy each received slice into the carry buffer, then hand the ring buffer straight back.
    private static unsafe int Drain(Connection conn, in RecvSnapshot snap, MonoxideContext ctx, int carryLen)
    {
        while (conn.TryGetItem(snap, out var item))
        {
            if (item.HasBuffer)
            {
                ReadOnlySpan<byte> slice = item.AsSpan();
                if (ctx.Carry.Length < carryLen + slice.Length)
                    Array.Resize(ref ctx.Carry, Math.Max(carryLen + slice.Length, ctx.Carry.Length * 2));
                slice.CopyTo(ctx.Carry.AsSpan(carryLen));
                carryLen += slice.Length;
            }
            conn.ReturnBuffer(in item);
        }
        return carryLen;
    }

    // Parse + dispatch every complete request in carry[0..carryLen]; returns the leftover length
    // (any trailing partial request is compacted to the front of the carry for the next read).
    private static async ValueTask<int> ParseAll(MonoxideContext ctx, FrozenRouter router, int carryLen, ParserLimits limits)
    {
        byte[] carry = ctx.Carry;
        int offset = 0;

        while (offset < carryLen)
        {
            ctx.ResetForRequest();
            ReadOnlyMemory<byte> mem = carry.AsMemory(offset, carryLen - offset);

            try
            {
                if (!UltraHardenedParser.TryExtractFullHeaderROM(ref mem, ctx.Req, in limits, out int bytesRead))
                    break;   // header not complete yet — wait for the next read

                int bodyStart = offset + bytesRead + 1;
                if (!TryResolveBody(ctx, carryLen, bodyStart, out int nextOffset, out bool needMore))
                {
                    if (needMore)
                        break;   // body not complete yet — wait
                    ctx.Status("400 Bad Request"u8);   // malformed framing
                    ctx.WantClose = true;
                    offset = carryLen;
                    break;
                }

                ctx.Close = WantsClose(ctx);

                // Resolve the handler + any captured {param} up front — no span survives the await below.
                MonoxideAsyncHandler? handler = router.Match(ctx.Req.Method.Span, ctx.Req.Path.Span, out int paramStart);
                ctx.ParamStart = paramStart;

                if (handler is not null)
                    await handler(ctx);   // synchronous for sync routes; truly suspends only for async ones
                else
                    ctx.Status("404 Not Found"u8);

                if (ctx.Close)
                    ctx.WantClose = true;

                offset = nextOffset;
            }
            catch (HttpParseException ex)
            {
                // Protocol/semantic violation — answer with the parser's status code and close.
                ctx.Status(ex.StatusCode == 431 ? "431 Request Header Fields Too Large"u8 : "400 Bad Request"u8);
                ctx.WantClose = true;
                offset = carryLen;
                break;
            }
        }

        int leftover = carryLen - offset;
        if (offset > 0 && leftover > 0)
            Array.Copy(carry, offset, carry, 0, leftover);
        return leftover;
    }

    // Resolves the request body and the offset of the next pipelined request. Returns false when the
    // body isn't fully buffered yet (needMore = true) or the framing is malformed (needMore = false).
    private static bool TryResolveBody(MonoxideContext ctx, int carryLen, int bodyStart, out int nextOffset, out bool needMore)
    {
        needMore = false;
        nextOffset = bodyStart;
        BodyFramingResult framing = BodyFramingDetector.DetectBodyFraming(ctx.Req);

        switch (framing.Framing)
        {
            case BodyFraming.None:
                ctx.Body = default;
                return true;

            case BodyFraming.ContentLength:
                long cl = framing.ContentLength;
                if (carryLen - bodyStart < cl) { needMore = true; return false; }
                ctx.Body = ctx.Carry.AsMemory(bodyStart, (int)cl);
                nextOffset = bodyStart + (int)cl;
                return true;

            case BodyFraming.Chunked:
                return TryDecodeChunked(ctx, carryLen, bodyStart, out nextOffset, out needMore);

            default:
                ctx.Body = default;
                return true;
        }
    }

    // Decode a chunked body into the context's decode buffer; sets ctx.Body to the decoded payload.
    private static bool TryDecodeChunked(MonoxideContext ctx, int carryLen, int bodyStart, out int nextOffset, out bool needMore)
    {
        needMore = false;
        nextOffset = bodyStart;
        var decoder = new ChunkedBodyStream();
        int pos = bodyStart;
        int decoded = 0;

        while (true)
        {
            ChunkResult r = decoder.TryReadChunk(
                ctx.Carry.AsSpan(pos, carryLen - pos), out int consumed, out int dataOffset, out int dataLength);

            switch (r)
            {
                case ChunkResult.Chunk:
                    if (ctx.BodyBuf.Length < decoded + dataLength)
                        Array.Resize(ref ctx.BodyBuf, Math.Max(decoded + dataLength, ctx.BodyBuf.Length * 2));
                    ctx.Carry.AsSpan(pos + dataOffset, dataLength).CopyTo(ctx.BodyBuf.AsSpan(decoded));
                    decoded += dataLength;
                    pos += consumed;
                    continue;

                case ChunkResult.Completed:
                    pos += consumed;
                    ctx.Body = ctx.BodyBuf.AsMemory(0, decoded);
                    nextOffset = pos;
                    return true;

                case ChunkResult.NeedMoreData:
                    needMore = true;
                    return false;

                default:
                    return false;   // malformed
            }
        }
    }

    private static bool WantsClose(MonoxideContext ctx)
        => ctx.TryHeader("connection"u8, out ReadOnlySpan<byte> v)
           && MonoxideContext.AsciiEqualsIgnoreCase(v, "close"u8);
}
