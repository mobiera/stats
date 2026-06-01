# stats

A scalable [Quarkus](https://quarkus.io/) container that centralizes service statistics.

It consumes statistic events from an ActiveMQ Artemis queue, aggregates them into time‑bucketed counters (hour / day / month) per *stat class* and *entity*, persists them in PostgreSQL, and exposes a read‑only REST API to query and compare those counters.

## Architecture

```text
  producers (other services)
        │  StatEvent
        ▼
  Artemis queue "stats-queue"  (replicated across 1..8 broker instances)
        │
        ▼
  stats container ──► aggregates ──► PostgreSQL ("stat" table)
        │
        ▼
  REST query API (/stats/*) + Swagger UI
```

- **Ingest**: a multi‑threaded JMS consumer reads `StatEvent` messages from `stats-queue` and increments the matching counters.
- **Aggregation**: each event targets a *stat class* (e.g. `ANIMATOR`), an *entity id* (preferably a UUID) and one or more *stat enums* (the individual counters). Values are accumulated per granularity (`HOUR`, `DAY`, `MONTH`).
- **Storage**: counters are stored in the `stat` table (one row per stat class / entity / granularity / timestamp), keyed by a deterministic id.
- **Query**: clients call the REST API to read, sum or compare counters over a time range.

## Requirements

- Java 25 (the container image is built from `maven:3-eclipse-temurin-25`)
- Maven (a wrapper `./mvnw` is provided)
- PostgreSQL
- ActiveMQ Artemis (shared with the rest of the platform)

## Configuration

Configuration is provided through `src/main/resources/application.properties` and can be overridden with environment variables.

### Application

| Property | Env var | Default | Description |
|----------|---------|---------|-------------|
| `quarkus.http.port` | `QUARKUS_HTTP_PORT` | `8700` (dev) | HTTP port. |
| `com.mobiera.ms.commons.app.name` | — | `STATS` | Application name used in logs. |
| `com.mobiera.ms.commons.stats.threads` | — | `32` | Number of consumer threads (per Artemis instance). Tune to available resources. |
| `com.mobiera.ms.commons.stats.timezone` | — | `America/Bogota` | Timezone used to compute time buckets. |
| `com.mobiera.ms.commons.stats.in.memory.past.units` | — | `5` | Number of past units kept in memory for fast accumulation. |
| `com.mobiera.ms.commons.stats.sleep.per.flush.millis` | — | `60000` | Delay between flushes of in‑memory counters to the database. |
| `com.mobiera.ms.commons.stats.debug` | — | `false` | Enables verbose logging. |

### Queue

| Property | Default | Description |
|----------|---------|-------------|
| `com.mobiera.ms.commons.stats.jms.queue.name` | `stats-queue` | Queue the events are read from. |
| `com.mobiera.ms.commons.stats.jms.incoming.ttl` | `86400000` | TTL (ms) of incoming messages. |
| `com.mobiera.ms.commons.stats.jms.retry.delay` | `10000` | Retry delay (ms). |
| `com.mobiera.ms.commons.stats.jms.ex.delay` | `10000` | Delay (ms) after an exception. |
| `com.mobiera.artemis.producer.poolsize` | `16` | Producer pool size. |

### Database

| Property | Env var | Description |
|----------|---------|-------------|
| `quarkus.datasource.db-kind` | — | Database kind (`postgresql`). |
| `quarkus.datasource.username` | — | DB username. |
| `quarkus.datasource.password` | `QUARKUS_DATASOURCE_PASSWORD` | DB password (never commit it). |
| `quarkus.datasource.jdbc.url` | `QUARKUS_DATASOURCE_JDBC_URL` | JDBC URL. |

The schema is managed by Hibernate (`quarkus.hibernate-orm.database.generation=update`).

### Artemis (1 to 8 instances)

Statistics queues can be spread across **1 to 8 Artemis broker instances**. This decision is **global to the platform**: every Mobiera service (all `mno-adapters-*`, `service-log`, `stats`…) **must share the same Artemis configuration**, because all queues are replicated across all configured instances.

Select the number of instances:

```properties
com.mobiera.ms.mno.quarkus.artemis.instances=1
```

Configure each instance (`a0` … `a7`):

```properties
quarkus.artemis."a0".url=tcp://artemis:61616
quarkus.artemis."a0".username=quarkus
quarkus.artemis."a0".password=...

quarkus.artemis."a1".url=tcp://artemis:61616
quarkus.artemis."a1".username=quarkus
quarkus.artemis."a1".password=...
# ... up to a7
```

> **Note:** the Artemis credentials configured here must be able to connect to the broker(s).

On Kubernetes, use the equivalent environment variables:

```bash
COM_MOBIERA_MS_MNO_QUARKUS_ARTEMIS_INSTANCES=...

QUARKUS_ARTEMIS__A0__URL=...
QUARKUS_ARTEMIS__A0__USERNAME=...
QUARKUS_ARTEMIS__A0__PASSWORD=...

QUARKUS_ARTEMIS__A1__URL=...
# ...
```

## REST API

The query API is documented with Swagger / OpenAPI. In dev mode the UI is available at:

```
http://localhost:8700/q/swagger-ui/
```

All endpoints accept and return JSON.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/stats/view` | Returns a statistics view over a time range. Can return cumulative values. |
| `POST` | `/stats/compare` | Compares several KPIs over a time range. |
| `POST` | `/stats/sumLastNStatVO` | Returns the sum of a counter over the last *N* buckets. |
| `POST` | `/stats/get` | Returns a single counter for a given stat class / entity / granularity / timestamp. |

The granularity (`HOUR`, `DAY`, `MONTH`) is computed automatically from the requested range.

## Producing statistics

Other services publish `StatEvent` messages to `stats-queue`. The typical producer accumulates counters like this:

```java
statProducer.spool(
    FriendChatClass.ANIMATOR.toString(), // stat class
    entityId,                            // entity id (preferably a UUID)
    AnimatorStat.SENT_MSGS,              // counter to increment
    Instant.now(),                       // timestamp
    1);                                  // increment
```

The shared `StatEvent` / `StatEnum` types come from the `stats-api` dependency. Access to that artifact (and to `mobiera-commons`) requires the appropriate Maven repository credentials, configured either in `settings.xml` or as environment variables.

## Build & run

Dev mode (live reload, Swagger UI on port 8700):

```bash
./mvnw quarkus:dev
```

Package the application:

```bash
./mvnw package
# runnable with:
java -jar target/quarkus-app/quarkus-run.jar
```

Native executable:

```bash
./mvnw package -Pnative
# or, without a local GraalVM:
./mvnw package -Pnative -Dquarkus.native.container-build=true
```

## Container image

CI builds and pushes the image to Docker Hub as `mobiera/stats` (`src/main/docker/Dockerfile.jvm`).

- Push to `main` → tag `main`
- Push to `dev` → tag `dev`
- Release `vX.Y.Z` → tag `X.Y.Z`

Run locally:

```bash
docker run --rm -p 8700:8080 \
  -e QUARKUS_DATASOURCE_JDBC_URL=jdbc:postgresql://db/aircast \
  -e QUARKUS_DATASOURCE_USERNAME=aircast \
  -e QUARKUS_DATASOURCE_PASSWORD=... \
  -e ARTEMIS_HOST=artemis \
  -e ARTEMIS_PASSWORD=... \
  mobiera/stats:main
```

## License

Apache License 2.0 — see [LICENSE](./LICENSE).
