using System.Buffers.Text;
using System.IO.Compression;

using Glyph11.Protocol;

namespace Monoxide;

/// <summary>
/// Per-connection request/response context, reused across requests on a connection and pooled per
/// reactor. The request is a zero-copy view over the recv carry buffer (Glyph11's
/// <see cref="BinaryRequest"/> slices point straight into it); the response is built into
/// <see cref="Out"/> and drained into the write slab by <see cref="ConnectionLoop"/>. Reactor-thread
/// affine — never touched by more than one reactor, so nothing here needs synchronization.
/// </summary>
public sealed class MonoxideContext
{
    // The parsed request (Method/Path/Query/Headers are ReadOnlyMemory<byte> views into Carry).
    internal readonly BinaryRequest Req = new();

    // Accumulates recv slices until a full request (or several, pipelined) can be parsed out of it.
    internal byte[] Carry = new byte[16 * 1024];

    // The framed response bytes (status + headers + body) for the current read batch.
    internal byte[] Out = new byte[8 * 1024];
    internal int OutLen;

    // Body-building scratch: handlers Append(...) here, then frame it with Json().
    private byte[] _scratch = new byte[8 * 1024];
    private int _scratchLen;

    // Holds a decoded chunked body (Content-Length bodies are viewed in-place in Carry instead).
    internal byte[] BodyBuf = new byte[4 * 1024];

    // Per-request brotli output for JsonBrotli (Content-Encoding: br).
    private byte[] _br = new byte[16 * 1024];

    /// <summary>True when the client asked to close after this request (Connection: close).</summary>
    internal bool Close;

    /// <summary>Set by a handler/loop to end the connection after the in-flight response is flushed.</summary>
    internal bool WantClose;

    // Byte offset of a captured route {param} within Path (set by the loop from the matched route), or -1.
    internal int ParamStart = -1;

    /// <summary>The request body bytes (Content-Length view or decoded chunked payload). Empty if none.</summary>
    public ReadOnlyMemory<byte> Body { get; internal set; }

    /// <summary>Request method, e.g. "GET" — a zero-copy view.</summary>
    public ReadOnlySpan<byte> Method => Req.Method.Span;

    /// <summary>Request path without the query string, e.g. "/json/5" — a zero-copy view.</summary>
    public ReadOnlySpan<byte> Path => Req.Path.Span;

    /// <summary>Looks up a query-string parameter by name (exact, case-sensitive key).</summary>
    public bool TryQuery(ReadOnlySpan<byte> name, out ReadOnlySpan<byte> value)
    {
        KeyValueList q = Req.QueryParameters;
        for (int i = 0; i < q.Count; i++)
        {
            var kv = q[i];
            if (kv.Key.Span.SequenceEqual(name)) { value = kv.Value.Span; return true; }
        }
        value = default;
        return false;
    }

    /// <summary>The captured trailing route parameter (e.g. "5" for "/json/{count}" matching "/json/5").</summary>
    public ReadOnlySpan<byte> RouteParam => ParamStart >= 0 ? Path[ParamStart..] : default;

    /// <summary>Parses the captured route parameter as an int (the whole captured segment must be digits).</summary>
    public bool TryRouteInt(out int value)
    {
        ReadOnlySpan<byte> p = RouteParam;
        return Utf8Parser.TryParse(p, out value, out int used) && used == p.Length && p.Length > 0;
    }

    /// <summary>True when the client's Accept-Encoding lists brotli ("br").</summary>
    public bool AcceptsBrotli => TryHeader("accept-encoding"u8, out ReadOnlySpan<byte> v) && v.IndexOf("br"u8) >= 0;

    /// <summary>True when the client's Accept-Encoding lists gzip.</summary>
    public bool AcceptsGzip => TryHeader("accept-encoding"u8, out ReadOnlySpan<byte> v) && v.IndexOf("gzip"u8) >= 0;

    /// <summary>Looks up a header by name (case-insensitive).</summary>
    public bool TryHeader(ReadOnlySpan<byte> name, out ReadOnlySpan<byte> value)
    {
        KeyValueList h = Req.Headers;
        for (int i = 0; i < h.Count; i++)
        {
            var kv = h[i];
            if (AsciiEqualsIgnoreCase(kv.Key.Span, name)) { value = kv.Value.Span; return true; }
        }
        value = default;
        return false;
    }

    // ──────────────────────────────── response framing ────────────────────────────────

    /// <summary>Writes a complete <c>text/plain</c> 200 response (status + headers + body), keep-alive aware.</summary>
    public void Text(ReadOnlySpan<byte> body)
    {
        AppendOut("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: "u8);
        AppendOutLong(body.Length);
        AppendOut(Close ? "\r\nConnection: close\r\n\r\n"u8 : "\r\n\r\n"u8);
        AppendOut(body);
    }

    /// <summary>Appends raw bytes to the body scratch (pair with <see cref="Json"/>).</summary>
    public void Append(ReadOnlySpan<byte> bytes)
    {
        EnsureScratch(_scratchLen + bytes.Length);
        bytes.CopyTo(_scratch.AsSpan(_scratchLen));
        _scratchLen += bytes.Length;
    }

    /// <summary>Appends a decimal integer to the body scratch.</summary>
    public void AppendInt(long value)
    {
        EnsureScratch(_scratchLen + 20);
        Utf8Formatter.TryFormat(value, _scratch.AsSpan(_scratchLen), out int n);
        _scratchLen += n;
    }

    /// <summary>Frames everything appended to the body scratch as an <c>application/json</c> 200 response.</summary>
    public void Json()
    {
        AppendOut("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "u8);
        AppendOutLong(_scratchLen);
        AppendOut(Close ? "\r\nConnection: close\r\n\r\n"u8 : "\r\n\r\n"u8);
        AppendOut(_scratch.AsSpan(0, _scratchLen));
        _scratchLen = 0;
    }

    /// <summary>
    /// Frames the body scratch as a brotli-compressed <c>application/json</c> 200 response
    /// (<c>Content-Encoding: br</c>) — for clients that sent <c>Accept-Encoding: br</c>.
    /// </summary>
    public void JsonBrotli()
    {
        int max = BrotliEncoder.GetMaxCompressedLength(_scratchLen);
        if (_br.Length < max) Array.Resize(ref _br, Math.Max(max, _br.Length * 2));
        BrotliEncoder.TryCompress(_scratch.AsSpan(0, _scratchLen), _br, out int written, quality: 1, window: 22);

        AppendOut("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Encoding: br\r\nContent-Length: "u8);
        AppendOutLong(written);
        AppendOut(Close ? "\r\nConnection: close\r\n\r\n"u8 : "\r\n\r\n"u8);
        AppendOut(_br.AsSpan(0, written));
        _scratchLen = 0;
    }

    /// <summary>Writes a bare status response with <c>Content-Length: 0</c>, e.g. <c>Status("404 Not Found"u8)</c>.</summary>
    public void Status(ReadOnlySpan<byte> statusText)
    {
        AppendOut("HTTP/1.1 "u8);
        AppendOut(statusText);
        AppendOut("\r\nContent-Length: 0\r\n"u8);
        AppendOut(Close ? "Connection: close\r\n\r\n"u8 : "\r\n"u8);
    }

    // Appends an already-framed response (status + headers + body) verbatim — used by static serving to
    // write ioxide.file's baked native response straight through the slab path.
    internal void WriteRaw(ReadOnlySpan<byte> bakedResponse) => AppendOut(bakedResponse);

    // Frames a static 200 with an explicit content-type, optional content-encoding (+ Vary), and a body.
    internal void WriteStatic(ReadOnlySpan<byte> contentType, ReadOnlySpan<byte> encoding, ReadOnlySpan<byte> body)
    {
        AppendOut("HTTP/1.1 200 OK\r\nContent-Type: "u8);
        AppendOut(contentType);
        if (!encoding.IsEmpty)
        {
            AppendOut("\r\nContent-Encoding: "u8);
            AppendOut(encoding);
            AppendOut("\r\nVary: Accept-Encoding"u8);
        }
        AppendOut("\r\nContent-Length: "u8);
        AppendOutLong(body.Length);
        AppendOut(Close ? "\r\nConnection: close\r\n\r\n"u8 : "\r\n\r\n"u8);
        AppendOut(body);
    }

    // ──────────────────────────────── internals ────────────────────────────────

    internal void AppendOut(ReadOnlySpan<byte> d)
    {
        if (Out.Length < OutLen + d.Length)
            Array.Resize(ref Out, Math.Max(OutLen + d.Length, Out.Length * 2));
        d.CopyTo(Out.AsSpan(OutLen));
        OutLen += d.Length;
    }

    private void AppendOutLong(long v)
    {
        Span<byte> num = stackalloc byte[20];
        Utf8Formatter.TryFormat(v, num, out int n);
        AppendOut(num[..n]);
    }

    private void EnsureScratch(int needed)
    {
        if (_scratch.Length < needed)
            Array.Resize(ref _scratch, Math.Max(needed, _scratch.Length * 2));
    }

    internal void ResetForRequest()
    {
        Req.Clear();
        _scratchLen = 0;
        Body = default;
        Close = false;
        ParamStart = -1;
    }

    internal void ResetForConnection()
    {
        OutLen = 0;
        WantClose = false;
        ResetForRequest();
    }

    internal static bool AsciiEqualsIgnoreCase(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
        {
            byte x = a[i], y = b[i];
            if (x is >= (byte)'A' and <= (byte)'Z') x += 32;
            if (y is >= (byte)'A' and <= (byte)'Z') y += 32;
            if (x != y) return false;
        }
        return true;
    }
}
