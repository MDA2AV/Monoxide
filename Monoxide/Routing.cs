namespace Monoxide;

/// <summary>A request handler: reads from the context and writes the response into it.</summary>
public delegate void MonoxideHandler(MonoxideContext ctx);

internal enum Verb : byte { Get, Post, Put, Any }

internal readonly struct Route(Verb verb, byte[] path, bool prefix, MonoxideHandler handler)
{
    public readonly Verb Verb = verb;
    public readonly byte[] Path = path;
    public readonly bool Prefix = prefix;   // match by StartsWith rather than exact
    public readonly MonoxideHandler Handler = handler;
}

/// <summary>
/// Immutable routing table, built once and shared read-only across every reactor (shared-nothing:
/// never mutated after <see cref="MonoxideApp.Run"/>). Matching is a linear scan over a handful of
/// routes — branch-predictable and cache-friendly, and free of the per-request dictionary lookups a
/// general framework pays. A perfect-hash/trie can replace this later without touching the API.
/// </summary>
internal sealed class FrozenRouter(Route[] routes, MonoxideHandler? fallback)
{
    private readonly Route[] _routes = routes;
    private readonly MonoxideHandler? _fallback = fallback;

    public MonoxideHandler? Match(ReadOnlySpan<byte> method, ReadOnlySpan<byte> path)
    {
        Verb v = VerbOf(method);
        for (int i = 0; i < _routes.Length; i++)
        {
            ref readonly Route r = ref _routes[i];
            if (r.Verb != Verb.Any && r.Verb != v) continue;
            if (r.Prefix ? path.StartsWith(r.Path) : path.SequenceEqual(r.Path))
                return r.Handler;
        }
        return _fallback;
    }

    internal static Verb VerbOf(ReadOnlySpan<byte> m) => m switch
    {
        [(byte)'G', (byte)'E', (byte)'T']               => Verb.Get,
        [(byte)'P', (byte)'O', (byte)'S', (byte)'T']     => Verb.Post,
        [(byte)'P', (byte)'U', (byte)'T']               => Verb.Put,
        _                                               => Verb.Any,
    };
}
