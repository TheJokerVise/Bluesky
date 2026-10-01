// PASSO 2 - Ingestor: legge da Bluesky Jetstream e scrive su Redpanda.
//
//   Jetstream (WebSocket) --> [questo programma] --> raw.bsky.posts / raw.bsky.likes / raw.bsky.reposts
//
// Uso:  dotnet run --project src/Bluesky.Ingestor [secondi]
//       (senza argomento gira finché non premi Ctrl+C)

using System.Buffers;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Text.Json;
using Bluesky.Ingestor;
using Confluent.Kafka;

// Questa volta chiediamo a Jetstream SOLO i tre tipi che ci servono: il filtro lo fa il server,
// così non scarichiamo follow, blocchi e il resto per poi buttarli.
const string DefaultJetstreamUrl =
    "wss://jetstream2.us-east.bsky.network/subscribe" +
    "?wantedCollections=app.bsky.feed.post" +
    "&wantedCollections=app.bsky.feed.like" +
    "&wantedCollections=app.bsky.feed.repost";

var jetstreamUrl = new Uri(Environment.GetEnvironmentVariable("JETSTREAM_URL") ?? DefaultJetstreamUrl);
// localhost:19092 è l'indirizzo "esterno" del broker, quello per le app sul tuo PC (vedi docker-compose.yml).
var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:19092";
int? runSeconds = args.Length > 0 && int.TryParse(args[0], out var s) ? s : null;

using var cts = new CancellationTokenSource();
if (runSeconds is { } seconds) cts.CancelAfter(TimeSpan.FromSeconds(seconds));
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

await KafkaSetup.EnsureTopicsAsync(bootstrapServers);

// --- Il producer -----------------------------------------------------------------------
var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrapServers,

    // Il broker ci conferma il messaggio solo quando è stato scritto davvero (Acks.All = da tutte
    // le repliche; con una sola replica equivale a "scritto"). Meno veloce, ma niente perdite silenziose.
    Acks = Acks.All,

    // Se un invio va in timeout e il producer riprova, senza questo flag il messaggio potrebbe
    // finire scritto due volte. Con l'idempotenza il broker riconosce i duplicati e li scarta.
    EnableIdempotence = true,

    // Non spediamo ogni messaggio da solo: il producer li accumula per 20 ms e li invia in blocco.
    // Poca latenza in più, molta più efficienza (meno viaggi di rete).
    LingerMs = 20,

    // I blocchi viaggiano compressi: il JSON si comprime molto bene.
    CompressionType = CompressionType.Zstd,
};

long read = 0, skipped = 0, delivered = 0, failed = 0;

using var producer = new ProducerBuilder<string, byte[]>(producerConfig)
    .SetErrorHandler((_, e) => Console.WriteLine($"[kafka] {e.Reason}"))
    .Build();

// Produce() NON aspetta il broker: mette il messaggio in una coda in memoria e torna subito.
// Quando il broker risponde, il producer chiama questa funzione ("delivery report").
void OnDelivered(DeliveryReport<string, byte[]> report)
{
    if (report.Error.IsError)
    {
        Interlocked.Increment(ref failed);
        Console.WriteLine($"[kafka] consegna fallita: {report.Error.Reason}");
    }
    else
    {
        Interlocked.Increment(ref delivered);
    }
}

// --- Il ciclo di lettura -----------------------------------------------------------------
using var ws = new ClientWebSocket();
Console.WriteLine($"Mi collego a Jetstream: {jetstreamUrl}");
await ws.ConnectAsync(jetstreamUrl, cts.Token);
Console.WriteLine("Collegato. Leggo da Bluesky e scrivo su Redpanda (Ctrl+C per fermare).\n");

var buffer = new ArrayBufferWriter<byte>();
var clock = Stopwatch.StartNew();
var lastReport = TimeSpan.Zero;
long lastRead = 0, lastDelivered = 0;

try
{
    while (ws.State == WebSocketState.Open)
    {
        buffer.Clear();
        ValueWebSocketReceiveResult result;
        do
        {
            result = await ws.ReceiveAsync(buffer.GetMemory(4096), cts.Token);
            buffer.Advance(result.Count);
        } while (!result.EndOfMessage);

        if (result.MessageType == WebSocketMessageType.Close) break;
        read++;

        var json = buffer.WrittenMemory;
        using var doc = JsonDocument.Parse(json);

        if (!EventMapper.TryMap(doc.RootElement, out var route) || route is null)
        {
            skipped++;
            continue;
        }

        var message = new Message<string, byte[]>
        {
            Key = route.Key,
            Value = json.ToArray(),                      // copia: il buffer viene riusato al giro dopo
            Timestamp = new Timestamp(route.EventTime),  // il momento in cui è successo, non quello in cui lo scriviamo
        };

        // Se il producer è più lento di Bluesky la coda interna si riempie. Invece di perdere
        // messaggi ci fermiamo un attimo (Poll lascia al producer il tempo di svuotarla) e riproviamo.
        // Questo si chiama "backpressure": chi è lento frena chi è veloce.
        while (true)
        {
            try
            {
                producer.Produce(route.Topic, message, OnDelivered);
                break;
            }
            catch (ProduceException<string, byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
            {
                cts.Token.ThrowIfCancellationRequested(); // Ctrl+C deve funzionare anche mentre aspettiamo
                producer.Poll(TimeSpan.FromMilliseconds(100));
            }
        }

        if (clock.Elapsed - lastReport >= TimeSpan.FromSeconds(5))
        {
            var span = (clock.Elapsed - lastReport).TotalSeconds;
            var d = Interlocked.Read(ref delivered);
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] letti {(read - lastRead) / span,5:F0}/s | " +
                              $"scritti su Redpanda {(d - lastDelivered) / span,5:F0}/s | " +
                              $"totale: letti {read:N0}, scartati {skipped:N0}, consegnati {d:N0}, falliti {Interlocked.Read(ref failed)}");
            lastRead = read;
            lastDelivered = d;
            lastReport = clock.Elapsed;
        }
    }
}
catch (OperationCanceledException)
{
    // Ctrl+C o scadenza dei secondi: fine normale.
}

// Prima di chiudere aspettiamo che tutti i messaggi ancora "in volo" arrivino al broker,
// altrimenti gli ultimi andrebbero persi.
Console.WriteLine("\nSvuoto la coda del producer...");
producer.Flush(TimeSpan.FromSeconds(10));

Console.WriteLine($"Fine. Letti {read:N0} | scartati {skipped:N0} | consegnati {Interlocked.Read(ref delivered):N0} | falliti {Interlocked.Read(ref failed)}");
