using Glyph11.Parser;

using ioxide;

namespace Monoxide;

/// <summary>
/// A Monoxide HTTP/1.1 application. Map routes fluently, then <see cref="Run"/> to host them on the
/// ioxide io_uring engine: one shared-nothing reactor per core, each its own thread. Requests are
/// parsed straight off the recv rings with Glyph11 and responses written straight into the write
/// slab — no <c>Stream</c>, no <c>Pipe</c>, no per-request allocation on the hot path.
/// </summary>
/// <example>
/// <code>
/// MonoxideApp.Create(config)
///     .MapGet("/pipeline", static ctx =&gt; ctx.Text("ok"u8))
///     .MapGet("/json/", JsonHandler)        // trailing '/' = prefix match
///     .MapFallback(BaselineHandler)
///     .Run();
/// </code>
/// </example>
public sealed class MonoxideApp
{
    private readonly ServerConfig _config;
    private readonly List<Route> _routes = [];
    private MonoxideAsyncHandler? _fallback;
    private Action<Reactor>? _onReactorStart;
    private ParserLimits _limits = ParserLimits.Default;

    private MonoxideApp(ServerConfig config) => _config = config;

    /// <summary>Creates an app with the given engine config (or a sensible default).</summary>
    public static MonoxideApp Create(ServerConfig? config = null) => new(config ?? Defaults());

    /// <summary>Maps a GET route. A trailing '/' prefix-matches; "{name}" captures the trailing segment (e.g. "/json/{count}").</summary>
    public MonoxideApp MapGet(string path, MonoxideHandler handler) => Map(Verb.Get, path, Wrap(handler));

    /// <summary>Maps an async GET route — for handlers that await (DB, cache, upstream).</summary>
    public MonoxideApp MapGet(string path, MonoxideAsyncHandler handler) => Map(Verb.Get, path, handler);

    /// <summary>Maps a POST route. A trailing '/' prefix-matches; "{name}" captures the trailing segment.</summary>
    public MonoxideApp MapPost(string path, MonoxideHandler handler) => Map(Verb.Post, path, Wrap(handler));

    /// <summary>Maps an async POST route.</summary>
    public MonoxideApp MapPost(string path, MonoxideAsyncHandler handler) => Map(Verb.Post, path, handler);

    /// <summary>Maps a PUT route. A trailing '/' prefix-matches; "{name}" captures the trailing segment.</summary>
    public MonoxideApp MapPut(string path, MonoxideHandler handler) => Map(Verb.Put, path, Wrap(handler));

    /// <summary>Maps an async PUT route.</summary>
    public MonoxideApp MapPut(string path, MonoxideAsyncHandler handler) => Map(Verb.Put, path, handler);

    /// <summary>Handler for any request no mapped route matched (e.g. the baseline sum / a 404 page).</summary>
    public MonoxideApp MapFallback(MonoxideHandler handler) { _fallback = Wrap(handler); return this; }

    /// <summary>Async fallback handler.</summary>
    public MonoxideApp MapFallback(MonoxideAsyncHandler handler) { _fallback = handler; return this; }

    /// <summary>
    /// Serves files under <paramref name="directory"/> at <paramref name="mountPrefix"/> (e.g. "/static"),
    /// backed by ioxide.file: per-file baked native responses + precompressed (.br/.gz) negotiation.
    /// </summary>
    public MonoxideApp MapStatic(string mountPrefix, string directory)
    {
        mountPrefix = "/" + mountPrefix.Trim('/');          // normalise to "/static"
        var statics = new MonoxideStatic(mountPrefix, directory);
        return MapGet(mountPrefix + "/", statics.Handle);   // trailing '/' → prefix match
    }

    /// <summary>Per-reactor startup hook — register ring-native, per-reactor services here (db pools, etc.).</summary>
    public MonoxideApp OnReactorStart(Action<Reactor> hook) { _onReactorStart = hook; return this; }

    /// <summary>Overrides the Glyph11 parser limits (header counts/sizes, URL length, ...).</summary>
    public MonoxideApp WithParserLimits(ParserLimits limits) { _limits = limits; return this; }

    // A sync handler is wrapped once here (at map time, never per request) into the async form the loop
    // awaits; it returns a completed ValueTask, so the await finishes synchronously with no allocation.
    private static MonoxideAsyncHandler Wrap(MonoxideHandler sync) => ctx => { sync(ctx); return default; };

    private MonoxideApp Map(Verb verb, string path, MonoxideAsyncHandler handler)
    {
        // "/json/{count}" → literal "/json/" (prefix-matched); the trailing segment is captured as the param.
        int brace = path.IndexOf('{');
        bool hasParam = brace >= 0;
        string literal = hasParam ? path[..brace] : path;
        bool prefix = hasParam || (literal.Length > 1 && literal[^1] == '/');
        byte[] lit = System.Text.Encoding.ASCII.GetBytes(literal);
        int paramStart = hasParam ? lit.Length : -1;
        _routes.Add(new Route(verb, lit, prefix, paramStart, handler));
        return this;
    }

    /// <summary>
    /// Freezes the route table and runs the engine: spawns <c>ReactorCount</c> reactor threads, each
    /// serving connections through the per-connection loop, and blocks until they exit. The router is
    /// built once here and shared read-only — no reactor ever mutates it.
    /// </summary>
    public void Run()
    {
        var router = new FrozenRouter(_routes.ToArray(), _fallback);
        int slab = _config.WriteSlabSize;
        ParserLimits limits = _limits;

        var threads = new Thread[_config.ReactorCount];
        for (int i = 0; i < _config.ReactorCount; i++)
        {
            var reactor = new Reactor(i, _config);
            if (_onReactorStart is not null)
                reactor.OnStart = _onReactorStart;
            reactor.Handle = (rr, conn) => ConnectionLoop.HandleAsync(rr, conn, router, slab, limits);
            threads[i] = new Thread(reactor.Run) { Name = $"monoxide-r{i}", IsBackground = false };
            threads[i].Start();
        }
        foreach (Thread t in threads)
            t.Join();
    }

    private static ServerConfig Defaults() => new()
    {
        Port = 8080,
        ReactorCount = Math.Min(Environment.ProcessorCount, 64),
        Incremental = false,
    };
}
