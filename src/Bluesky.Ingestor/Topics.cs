namespace Bluesky.Ingestor;

// I "raccoglitori" su Redpanda. Il prefisso "raw." dice che contengono il dato com'è arrivato
// da Bluesky, senza nessuna elaborazione: i passi successivi lo leggeranno e lo trasformeranno.
public static class Topics
{
    public const string Posts = "raw.bsky.posts";
    public const string Likes = "raw.bsky.likes";
    public const string Reposts = "raw.bsky.reposts";

    public static readonly string[] All = [Posts, Likes, Reposts];
}
