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
    private MonoxideHandler? _fallback;
    private Action<Reactor>? _onReactorStart;
    private ParserLimits _limits = ParserLimits.Default;

    private MonoxideApp(ServerConfig config) => _config = config;

    /// <summary>Creates an app with the given engine config (or a sensible default).</summary>
    public static MonoxideApp Create(ServerConfig? config = null) => new(config ?? Defaults());

    /// <summary>Maps a GET route. A path ending in '/' is a prefix match (e.g. "/json/" matches "/json/5").</summary>
    public MonoxideApp MapGet(string path, MonoxideHandler handler) => Map(Verb.Get, path, handler);

    /// <summary>Maps a POST route. A path ending in '/' is a prefix match.</summary>
    public MonoxideApp MapPost(string path, MonoxideHandler handler) => Map(Verb.Post, path, handler);

    /// <summary>Maps a PUT route. A path ending in '/' is a prefix match.</summary>
    public MonoxideApp MapPut(string path, MonoxideHandler handler) => Map(Verb.Put, path, handler);

    /// <summary>Handler for any request no mapped route matched (e.g. the baseline sum / a 404 page).</summary>
    public MonoxideApp MapFallback(MonoxideHandler handler) { _fallback = handler; return this; }

    /// <summary>Per-reactor startup hook — register ring-native, per-reactor services here (db pools, etc.).</summary>
    public MonoxideApp OnReactorStart(Action<Reactor> hook) { _onReactorStart = hook; return this; }

    /// <summary>Overrides the Glyph11 parser limits (header counts/sizes, URL length, ...).</summary>
    public MonoxideApp WithParserLimits(ParserLimits limits) { _limits = limits; return this; }

    private MonoxideApp Map(Verb verb, string path, MonoxideHandler handler)
    {
        bool prefix = path.Length > 1 && path[^1] == '/';
        _routes.Add(new Route(verb, System.Text.Encoding.ASCII.GetBytes(path), prefix, handler));
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
