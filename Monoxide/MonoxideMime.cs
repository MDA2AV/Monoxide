namespace Monoxide;

/// <summary>Minimal extension → Content-Type map for static serving (the types the file profiles use).</summary>
internal static class MonoxideMime
{
    public static ReadOnlySpan<byte> For(ReadOnlySpan<byte> path)
    {
        int dot = path.LastIndexOf((byte)'.');
        ReadOnlySpan<byte> e = dot < 0 ? default : path[(dot + 1)..];

        if (e.SequenceEqual("html"u8) || e.SequenceEqual("htm"u8)) return "text/html"u8;
        if (e.SequenceEqual("css"u8))                              return "text/css"u8;
        if (e.SequenceEqual("js"u8) || e.SequenceEqual("mjs"u8))   return "application/javascript"u8;
        if (e.SequenceEqual("json"u8))                             return "application/json"u8;
        if (e.SequenceEqual("svg"u8))                              return "image/svg+xml"u8;
        if (e.SequenceEqual("png"u8))                              return "image/png"u8;
        if (e.SequenceEqual("webp"u8))                             return "image/webp"u8;
        if (e.SequenceEqual("avif"u8))                             return "image/avif"u8;
        if (e.SequenceEqual("woff2"u8))                            return "font/woff2"u8;
        if (e.SequenceEqual("ico"u8))                              return "image/x-icon"u8;
        if (e.SequenceEqual("gif"u8))                              return "image/gif"u8;
        if (e.SequenceEqual("jpg"u8) || e.SequenceEqual("jpeg"u8)) return "image/jpeg"u8;
        if (e.SequenceEqual("txt"u8))                              return "text/plain"u8;
        return "application/octet-stream"u8;
    }
}
