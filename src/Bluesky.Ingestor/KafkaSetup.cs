using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Bluesky.Ingestor;

public static class KafkaSetup
{
    // Crea i topic se non esistono già. È sicuro lanciarlo a ogni avvio (idempotente).
    public static async Task EnsureTopicsAsync(string bootstrapServers)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

        var specs = Topics.All.Select(name => new TopicSpecification
        {
            Name = name,
            NumPartitions = 3,      // 3 "quaderni" per topic: fino a 3 consumer in parallelo
            ReplicationFactor = 1,  // 1 sola copia: abbiamo un solo broker (in produzione sarebbero 3)
            Configs = new Dictionary<string, string>
            {
                // I quaderni non crescono all'infinito: i messaggi più vecchi di 6 ore vengono eliminati.
                ["retention.ms"] = TimeSpan.FromHours(6).TotalMilliseconds.ToString("F0"),
            },
        });

        try
        {
            await admin.CreateTopicsAsync(specs);
            Console.WriteLine($"Topic creati: {string.Join(", ", Topics.All)}");
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code is ErrorCode.TopicAlreadyExists or ErrorCode.NoError))
        {
            // Alcuni (o tutti) i topic c'erano già: va bene così.
            Console.WriteLine("Topic già esistenti (o creati ora): ok.");
        }
    }
}
