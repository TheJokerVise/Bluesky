using System.Text.Json;

namespace Bluesky.Ingestor;

// Dove va scritto un evento (Topic), con quale chiave (Key) e quando è successo davvero (EventTime).
public sealed record Route(string Topic, string Key, DateTimeOffset EventTime);

public static class EventMapper
{
    private const string PostCollection = "app.bsky.feed.post";
    private const string LikeCollection = "app.bsky.feed.like";
    private const string RepostCollection = "app.bsky.feed.repost";

    // Restituisce true se l'evento ci interessa, e in "route" dice dove metterlo.
    // La chiave è SEMPRE l'indirizzo del post a cui l'evento si riferisce:
    //   - un post         -> il suo stesso indirizzo, ricostruito da did + collection + rkey
    //   - un like/repost  -> l'indirizzo del post citato in record.subject.uri
    // Così il post e tutti i suoi like/repost hanno la stessa chiave e, nei rispettivi topic,
    // finiscono nella stessa partizione (stessa chiave -> stessa partizione).
    public static bool TryMap(JsonElement root, out Route? route)
    {
        route = null;

        if (root.GetProperty("kind").GetString() != "commit") return false;

        var commit = root.GetProperty("commit");

        // Per ora ignoriamo update e delete. Un "like.delete" non contiene il subject, quindi
        // non potremmo nemmeno calcolare la chiave: è un problema che riprenderemo più avanti.
        if (commit.GetProperty("operation").GetString() != "create") return false;

        var collection = commit.GetProperty("collection").GetString();

        string topic;
        string? key;
        switch (collection)
        {
            case PostCollection:
                topic = Topics.Posts;
                key = $"at://{root.GetProperty("did").GetString()}/{collection}/{commit.GetProperty("rkey").GetString()}";
                break;
            case LikeCollection:
                topic = Topics.Likes;
                key = SubjectPostUri(commit);
                break;
            case RepostCollection:
                topic = Topics.Reposts;
                key = SubjectPostUri(commit);
                break;
            default:
                return false;
        }

        if (key is null) return false;

        // time_us è il momento in cui l'evento è avvenuto, in microsecondi dal 1970.
        var eventTime = DateTimeOffset.FromUnixTimeMilliseconds(root.GetProperty("time_us").GetInt64() / 1000);

        route = new Route(topic, key, eventTime);
        return true;
    }

    // Estrae record.subject.uri, ma solo se punta davvero a un post
    // (si può mettere like anche ad altre cose, come i feed: quelle le scartiamo).
    private static string? SubjectPostUri(JsonElement commit)
    {
        if (!commit.TryGetProperty("record", out var record)) return null;
        if (!record.TryGetProperty("subject", out var subject)) return null;
        if (!subject.TryGetProperty("uri", out var uriElement)) return null;

        var uri = uriElement.GetString();
        return uri is not null && uri.Contains($"/{PostCollection}/") ? uri : null;
    }
}
