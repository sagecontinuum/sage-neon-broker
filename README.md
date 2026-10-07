# Sage Neon Broker: Kafka mirror with MirrorMaker 2

This project starts a new single-broker Kafka cluster and a MirrorMaker 2 (MM2) process that copies the topics 
you choose from an existing Kafka server into it. In MM2's terms the existing server is the *source* (alias `primary` in the config) 
and the new broker is the *target* (alias `mirror`). Clients log in to the mirror with SASL/SCRAM usernames and passwords, 
and both containers expose Prometheus metrics, including per-user usage.

Keep all of these files in the same folder:

| File | What it is |
|---|---|
| `docker-compose.yml` | The two containers: `sage-kafka` (the broker) and `mirrormaker` (MM2). |
| `Dockerfile` | The official `apache/kafka` image plus the Prometheus JMX exporter agent.|
| `sage-kafka.properties` | The mirror broker's configuration: listeners, authentication, storage. |
| `mm2.properties` | MirrorMaker's configuration. This is the file you'll edit most. |
| `secrets.env` | The password MirrorMaker uses to log in to the mirror. |
| `jmx-sage-kafka.yml`, `jmx-mirrormaker.yml` | Rules that turn each process's JMX metrics into Prometheus metrics. |
| `mirror-explorer.ipynb` | A Jupyter notebook for looking at what's on the mirror (see [Exploring the mirror in a notebook](#exploring-the-mirror-in-a-notebook)). |
| `test-source.env` | An optional throwaway source broker, run as its own container outside Compose (see [Testing with a throwaway source](#testing-with-a-throwaway-source)). |

## 1. Before the first start

`mm2.properties` contains the current configuration to target a NEON Kafka server that is accessible to a Sage Thor node.
`secrets.env` is where you defined the `primary.bootstrap.servers` via `NEON_BOOTSTRAP_SERVER` and also the credentials for the server
since SASL/SCRAM usernames and passwords are also being used. For the podman deployment, `podman/mirror-secrets.yaml` is used instead 
of `secrets.env` and contains the same contents. Both files are in the `.gitignore` file.

## 2. Optional settings

These can go in a `.env` file next to `docker-compose.yml`.

| Variable | Default | Purpose |
|---|---|---|
| `MIRROR_ADVERTISED_HOST` | `localhost` | Hostname or IP that clients outside Docker use to reach the mirror. Set it to the Docker host's name if those clients run on other machines. |
| `MIRROR_EXTERNAL_PORT` | `9094` | Host port for those clients. |
| `MIRROR_METRICS_PORT` | `7071` | Host port for the broker's Prometheus metrics. |
| `MM2_METRICS_PORT` | `7072` | Host port for MirrorMaker's Prometheus metrics. |
| `KAFKA_IMAGE` | `apache/kafka:4.3.1` | Base image for both containers. |
| `MIRROR_CLUSTER_ID` | a fixed ID | Only used the first time the broker formats its storage. Changing it later requires wiping the volume. |

`log.retention.hours` in `sage-kafka.properties` (168 hours) applies to mirrored topics that don't set their own retention on the source, 
because MM2 copies per-topic settings but not the source broker's defaults.

## 3. Start it

```bash
#Docker compose
docker compose up -d --build
docker compose logs -f mirrormaker

#Podman (Sage Thor)
podman build -t localhost/kafka-mirror/kafka-jmx:local .
podman kube play podman/sage-kafka-pvc.yaml
podman kube play --configmap mirror-secrets.yaml sage-kafka.yaml
```

MM2 can take a minute or two after startup before records start flowing. On the first run it copies everything the source still retains for 
the selected topics, so large topics take a while. 

To copy only new data instead, uncomment `primary.consumer.auto.offset.reset = latest` in `mm2.properties` before the first start.

## 4. Check that it's working

Admin commands run inside the broker container use the loopback-only `LOCAL` listener on `127.0.0.1:9095`, which needs no password:

```bash
# Topics on the mirror
docker compose exec sage-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9095 --list

podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka \
  /opt/kafka/bin/kafka-topics.sh   --bootstrap-server 127.0.0.1:9095 --list

# Read a few records from a mirrored topic
docker compose exec sage-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 127.0.0.1:9095 --topic orders --max-messages 5

podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 127.0.0.1:9095 --topic reading.sensor.csat3b --max-messages 5

```

Offsets on the mirror won't equal the source's, so compare record counts per partition (end minus start) rather than raw offsets. Besides your topics you'll see `heartbeats`, `primary.checkpoints.internal`, and several `mm2-...internal` topics.

## Exploring the mirror in a notebook

`mirror-explorer.ipynb` connects to the mirror's external port as its own user and lets you list topics, look at the latest messages (with JSON values split into columns), see what arrived recently, and watch new records arrive.

## Users and authentication

The broker has four listeners. `INTERNAL` (port 9092, for containers on the Compose network, which is what MM2 uses) 
and `EXTERNAL` (port 9094, published for everything outside Docker) both require SASL/SCRAM-SHA-512. `LOCAL` (127.0.0.1:9095) 
and `CONTROLLER` (127.0.0.1:9093) require nothing, but they only listen on the container's loopback interface, so only processes
inside the container can use them. That's what makes the password-free admin commands above work.

Users live in the cluster's metadata, so you add and remove them without restarting anything:

```bash
# Add a user (or reset its password)
docker compose exec sage-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9095 \
  --alter --entity-type users --entity-name sage --add-config 'SCRAM-SHA-512=[password=sagepass]'

podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 127.0.0.1:9095 --alter --entity-type users --entity-name sage --add-config 'SCRAM-SHA-512=[password=sagepass]'

# List users
docker compose exec sage-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9095 \
  --describe --entity-type users

podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 127.0.0.1:9095 --describe --entity-type users

# Remove a user's password, so it can no longer log in
docker compose exec sage-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9095 \
  --alter --entity-type users --entity-name sage --delete-config 'SCRAM-SHA-512'

podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 127.0.0.1:9095 --alter --entity-type users --entity-name sage \
  --delete-config 'SCRAM-SHA-512'
```

Clients outside Docker connect to `localhost:9094` (or `MIRROR_ADVERTISED_HOST:MIRROR_EXTERNAL_PORT`) with 
settings like these, passed to the Kafka CLI tools with `--command-config client.properties`:

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="sage" password="sagepass";
```

## Metrics

Each container runs the Prometheus JMX exporter as a Java agent. The broker's metrics are at `http://localhost:7071/metrics` and MirrorMaker's at `http://localhost:7072/metrics` (or on the ports you set). A Prometheus scrape config for them:

```yaml
scrape_configs:
  - job_name: kafka-mirror
    static_configs:
      - targets: ["<docker-host>:7071"]
  - job_name: mirrormaker
    static_configs:
      - targets: ["<docker-host>:7072"]
```

The broker exports the usual Kafka metrics, such as bytes in and out per topic (`kafka_server_brokertopicmetrics_bytesin_total{topic="..."}`), under-replicated partitions, request latencies, and KRaft state. MirrorMaker exports replication latency and record age per mirrored partition (`kafka_mirror_source_replication_latency_ms`, `kafka_mirror_source_record_age_ms`), how far its consumers are behind the source (`kafka_mirror_consumer_records_lag_max`), checkpoint latency per consumer group, and the status of its connectors and tasks (`kafka_mirror_task_status{status="failed"}` is worth an alert).

### Per-user metrics

Kafka only records per-user usage once at least one client quota exists. This one-time command sets a default quota of 1 GiB/s per user, far above what a single broker like this will see, which switches the metrics on without throttling anyone:

```bash
docker compose exec sage-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9095 \
  --alter --entity-type users --entity-default \
  --add-config 'producer_byte_rate=1073741824,consumer_byte_rate=1073741824'

podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9095 \
  --alter --entity-type users --entity-default \
  --add-config 'producer_byte_rate=1073741824,consumer_byte_rate=1073741824'
```

From then on the broker exports `kafka_user_bytes_per_second{request="Produce|Fetch", user="..."}` and `kafka_user_throttle_time_ms` for every user that's active. MM2 shows up as `user="mm2"`, and the password-free admin tools as `user="ANONYMOUS"`. The rate covers roughly the last ten seconds, so for usage over time let Prometheus integrate it; for example, approximate bytes each user produced over the last hour:

```promql
avg_over_time(kafka_user_bytes_per_second{request="Produce"}[1h]) * 3600
```

To see it with your own eyes, create a user and a scratch topic, then produce as that user for 30 seconds:

```bash
docker compose exec sage-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9095 \
  --alter --entity-type users --entity-name sage --add-config 'SCRAM-SHA-512=[password=sagepass]'

podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 127.0.0.1:9095 --alter --entity-type users --entity-name sage \
  --add-config 'SCRAM-SHA-512=[password=sagepass]'

docker compose exec sage-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9095 \
  --create --topic sage-demo --partitions 1

podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9095 --create --topic sage-demo --partitions 1

# sage's client settings, written inside the container
docker compose exec -T sage-kafka sh -c 'cat > /tmp/sage.properties' <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="sage" password="sagepass";
EOF

podman exec kafka-mirror-sage-kafka sh -c 'cat > /tmp/sage.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="sage" password="sagepass";
EOF'

# About 2 MB/s as alice for 30 seconds, through the authenticated INTERNAL listener
docker compose exec sage-kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
  --bootstrap-server sage-kafka:9092 --command-config /tmp/sage.properties \
  --topic sage-demo --num-records 300000 --record-size 200 --throughput 10000

podman exec kafka-mirror-sage-kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
  --bootstrap-server 127.0.0.1:9092 --command-config /tmp/sage.properties \
  --topic sage-demo --num-records 300000 --record-size 200 --throughput 10000
```

While it runs, `curl -s localhost:7071/metrics | grep kafka_user_bytes_per_second` in another terminal shows a line for `user="sage"`. Delete the scratch topic afterwards with `kafka-topics.sh --bootstrap-server 127.0.0.1:9095 --delete --topic sage-demo`, run the same way as the other admin commands.

## Testing with a throwaway source

You can run the whole pipeline against a disposable broker before pointing MM2 at your real server. The test broker is a separate container started with `docker run`, not part of the Compose file. Its settings live in `test-source.env`, and switching MM2 over to it is a change in `mm2.properties`. The test source itself has no authentication; MM2 still logs in to the mirror as usual.

Do this before you start mirroring the real server, or be ready to wipe the mirror afterward. The test topics, consumer groups, and MM2's bookkeeping all land in the same mirror broker.

### 1. Switch MM2 to the test source (Local with Docker-compose)

At the bottom of `mm2.properties`, uncomment the four lines in the TEST MODE block. You don't need to touch the real settings above them. In a `.properties` file the last value for a key wins, so these lines override the source address, topic list, security protocol, and starting position.

### 2. Start the mirror broker, then the test source

The first command creates the `kafka-mirror` network that the test broker joins. Run the second from this folder so Docker finds `test-source.env`.

```bash
docker compose up -d --build sage-kafka

docker run -d --rm --name test-source-kafka --hostname test-source-kafka \
  --network kafka-mirror -p 127.0.0.1:29092:29092 \
  --env-file test-source.env apache/kafka:4.3.1
```

The test broker is reachable as `test-source-kafka:9092` from containers on the network, and at `localhost:29092` from tools on your machine.

### 3. Create a test topic, some records, and a consumer group

```bash
docker exec test-source-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic mm2-test-orders --partitions 3

docker exec test-source-kafka sh -c 'seq 1 1000 | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic mm2-test-orders'

# Read 400 records as group "mm2-test-app" so there are committed offsets to translate
docker exec test-source-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic mm2-test-orders --group mm2-test-app \
  --from-beginning --max-messages 400 > /dev/null
```

If the first command times out, the broker is still starting; wait a few seconds and run it again.

### 4. Start MirrorMaker

```bash
docker compose up -d
docker compose logs -f mirrormaker
```

If MirrorMaker was already running before the test source existed, run `docker compose restart mirrormaker` instead. It needs a restart to pick up the new source.

### 5. Check the mirror

```bash
# The topic should exist with 3 partitions
docker compose exec sage-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9095 --describe --topic mm2-test-orders

# The end offsets across the 3 partitions should add up to 1000
docker compose exec sage-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server 127.0.0.1:9095 --topic mm2-test-orders --time -1

# The consumer group should appear with translated positions
docker compose exec sage-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 127.0.0.1:9095 --describe --group mm2-test-app
```

The consumer group can take a minute or two to appear. Its positions on the mirror may trail the source's, because MM2 translates offsets conservatively; that's the at-least-once behavior you'd see in a real failover.

To watch live replication, rerun the producer command from step 3. The new records show up on the mirror within seconds, and `kafka_mirror_source_replication_latency_ms` on `localhost:7072/metrics` shows how long they took. Rerun the consumer command and the group's position on the mirror moves forward too.

### 6. Clean up before using the real source

```bash
docker stop test-source-kafka   # --rm deletes the container and its data
docker compose down -v          # wipes the mirror: test topics, groups, users, MM2's progress
```

Stop the test source first. Compose can't remove the `kafka-mirror` network while that container is still attached to it.

Then comment the TEST MODE lines out again and start normally. The wipe matters because the test source uses the same `primary` alias as your real server. Without it, the test topics and groups stay on the mirror, and MM2's saved progress would apply to any real topic that happens to share a test topic's name. The wipe also removes any users you created, and the next start recreates `mm2` from `secrets.env`.


