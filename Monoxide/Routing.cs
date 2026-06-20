namespace Monoxide;

/// <summary>A synchronous request handler: reads the request and writes the response into the context.</summary>
public delegate void MonoxideHandler(MonoxideContext ctx);

/// <summary>An asynchronous request handler — for routes that await (DB, cache, upstream calls).</summary>
public delegate ValueTask MonoxideAsyncHandler(MonoxideContext ctx);

internal enum Verb : byte { Get, Post, Put, Any }

internal readonly struct Route(Verb verb, byte[] path, bool prefix, int paramStart, MonoxideAsyncHandler handler)
{
    public readonly Verb Verb = verb;
    public readonly byte[] Path = path;
    public readonly bool Prefix = prefix;       // match by StartsWith rather than exact
    public readonly int ParamStart = paramStart; // byte offset of a captured {param} in the path, or -1
    public readonly MonoxideAsyncHandler Handler = handler;
}

/// <summary>
/// Immutable routing table, built once and shared read-only across every reactor (shared-nothing:
/// never mutated after <see cref="MonoxideApp.Run"/>). Matching is a linear scan over a handful of
/// routes — branch-predictable and cache-friendly, and free of the per-request dictionary lookups a
/// general framework pays. A perfect-hash/trie can replace this later without touching the API.
/// </summary>
internal sealed class FrozenRouter(Route[] routes, MonoxideAsyncHandler? fallback)
{
    private readonly Route[] _routes = routes;
    private readonly MonoxideAsyncHandler? _fallback = fallback;

    /// <summary>
    /// Returns the handler for (method, path) and, via <paramref name="paramStart"/>, the byte offset
    /// where a captured trailing <c>{param}</c> begins (-1 when the route has none / the fallback ran).
    /// </summary>
    public MonoxideAsyncHandler? Match(ReadOnlySpan<byte> method, ReadOnlySpan<byte> path, out int paramStart)
    {
        Verb v = VerbOf(method);
        for (int i = 0; i < _routes.Length; i++)
        {
            ref readonly Route r = ref _routes[i];
            if (r.Verb != Verb.Any && r.Verb != v) continue;
            if (r.Prefix ? path.StartsWith(r.Path) : path.SequenceEqual(r.Path))
            {
                paramStart = r.ParamStart;
                return r.Handler;
            }
        }
        paramStart = -1;
        return _fallback;
    }

    internal static Verb VerbOf(ReadOnlySpan<byte> m) => m switch
    {
        [(byte)'G', (byte)'E', (byte)'T']           => Verb.Get,
        [(byte)'P', (byte)'O', (byte)'S', (byte)'T'] => Verb.Post,
        [(byte)'P', (byte)'U', (byte)'T']           => Verb.Put,
        _                                           => Verb.Any,
    };
}
