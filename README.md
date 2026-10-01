# Bluesky Pulse

Piattaforma event-driven a microservizi per analizzare in tempo reale post, like e notizie
(Bluesky Jetstream + feed RSS). Stack: **.NET 10**, **Kafka (Redpanda)**, **React**, Docker → Kubernetes.

Il progetto si costruisce a passi, ognuno spiegato e verificabile.

## Roadmap

- [x] **Passo 0** – Terreno: solution .NET, Docker Compose con Redpanda + Console
- [x] **Passo 1** – Esplorare Jetstream: com'è fatto il dato reale (esempi in `samples/`)
- [x] **Passo 2** – Ingestor/producer: Jetstream → Redpanda (`raw.bsky.posts|likes|reposts`, chiave = URI del post)
- [ ] **Passo 3** – Primo consumer: consumer group, offset, commit
- [ ] Passi successivi: normalizzazione, storage, trending, frontend React, Kubernetes

## Struttura

```
Bluesky/
├─ global.json              fissa la versione dell'SDK .NET (10.0.401)
├─ Directory.Build.props    impostazioni comuni a tutti i progetti (net10.0, nullable...)
├─ Bluesky.slnx             la solution
├─ docker-compose.yml       infrastruttura locale (broker + console)
└─ src/                     i microservizi (uno per cartella)
```

## Infrastruttura locale

```bash
docker compose up -d      # avvia Redpanda + Console
docker compose ps         # stato
docker compose down       # ferma (i dati restano nel volume)
docker compose down -v    # ferma E cancella i dati
```

| Cosa | Indirizzo |
|---|---|
| Console web | http://localhost:8080 |
| Kafka dal PC (app .NET) | `localhost:19092` |
| Kafka tra container | `redpanda:9092` |
| Schema Registry | http://localhost:18081 |

## Comandi `rpk` utili (CLI del broker, dentro il container)

```bash
docker exec redpanda rpk topic list
docker exec redpanda rpk topic create <nome> -p 3
docker exec redpanda rpk topic describe <nome>
docker exec -i redpanda rpk topic produce <nome> -k <chiave>
docker exec redpanda rpk topic consume <nome> --offset start
docker exec redpanda rpk group list
docker exec redpanda rpk topic delete <nome>
```
