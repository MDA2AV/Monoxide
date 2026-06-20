using ioxide.file;

namespace Monoxide;

/// <summary>
/// Static-file serving backed by ioxide.file's <see cref="StaticAssets"/> — each file is a baked HTTP
/// response in native memory, shared across reactors. Precompressed negotiation: when the client
/// accepts it, the <c>.br</c> (then <c>.gz</c>) sibling is served with the base file's Content-Type +
/// the matching <c>Content-Encoding</c> + <c>Vary</c>; otherwise the identity baked response is written
/// verbatim. Registered via <see cref="MonoxideApp.MapStatic"/>.
/// </summary>
internal sealed unsafe class MonoxideStatic
{
    private readonly StaticAssets _assets;
    private readonly int _prefix;   // bytes to strip from the request path, e.g. "/static".Length

    public MonoxideStatic(string mountPrefix, string directory)
    {
        // 1 MB bake threshold: every file profile asset (largest ~300 KB) is fully baked into native
        // memory, so a hit is one span write into the slab — no fd read on the hot path.
        _assets = new StaticAssets(directory, maxCachedFileBytes: 1 << 20);
        _prefix = System.Text.Encoding.ASCII.GetByteCount(mountPrefix);
    }

    public int Count => _assets.Count;

    public void Handle(MonoxideContext ctx)
    {
        ReadOnlySpan<byte> path = ctx.Path;
        if (path.Length <= _prefix) { ctx.Status("404 Not Found"u8); return; }

        ReadOnlySpan<byte> file = path[_prefix..];   // e.g. "/app.js"

        // Precompressed first (br > gzip), then identity. The siblings live in the same asset cache.
        if (ctx.AcceptsBrotli && TryServeEncoded(ctx, file, ".br"u8, "br"u8)) return;
        if (ctx.AcceptsGzip   && TryServeEncoded(ctx, file, ".gz"u8, "gzip"u8)) return;

        if (_assets.TryGet(file, out AssetCache.Asset asset))
            ctx.WriteRaw(new ReadOnlySpan<byte>((void*)asset.Response, asset.ResponseLength));
        else
            ctx.Status("404 Not Found"u8);
    }

    // Serve the "<file><ext>" sibling (e.g. /app.js.br) with the BASE file's content-type + the encoding.
    private bool TryServeEncoded(MonoxideContext ctx, ReadOnlySpan<byte> file, ReadOnlySpan<byte> ext, ReadOnlySpan<byte> encoding)
    {
        Span<byte> key = stackalloc byte[file.Length + ext.Length];
        file.CopyTo(key);
        ext.CopyTo(key[file.Length..]);

        if (!_assets.TryGet(key, out AssetCache.Asset asset)) return false;

        // The sibling's baked block is header+body; the body is its trailing asset.Length bytes.
        nint body = asset.Response + (nint)(asset.ResponseLength - (int)asset.Length);
        ctx.WriteStatic(MonoxideMime.For(file), encoding, new ReadOnlySpan<byte>((void*)body, (int)asset.Length));
        return true;
    }
}
