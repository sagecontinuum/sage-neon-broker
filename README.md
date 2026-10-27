# Sage Neon Broker: Kafka mirror with MirrorMaker 2

This project starts a new single-broker Kafka cluster and a MirrorMaker 2 (MM2) process that copies the topics 
you choose from an existing Kafka server into it. In MM2's terms the existing server is the *source* (alias `primary` in the config) 
and the new broker is the *target* (alias `mirror`). Clients log in to the mirror with SASL/SCRAM usernames and passwords, 
and both containers expose Prometheus metrics, including per-user usage.

The files in this repository:

| File | What it is |
|---|---|
| `podman/sage-kafka.yaml` | The Podman pod with both containers: `sage-kafka` (the broker) and `mirrormaker` (MM2). |
| `podman/sage-kafka-pvc.yaml` | The Podman volume for the broker's data. |
| `k8s/sage-kafka.yaml`, `kustomization.yaml` | The same pod for Kubernetes, with its Service and NetworkPolicy. `kustomization.yaml` turns the config files below into ConfigMaps. |
| `Dockerfile` | The official `apache/kafka` image plus the Prometheus JMX exporter agent.|
| `sage-kafka.properties` | The mirror broker's configuration: listeners, authentication, storage. |
| `mm2.properties` | MirrorMaker's configuration. This is the file you'll edit most. |
| `podman/mirror-secrets.yaml`, `k8s/mirror-secrets.yaml` | The source's address and credentials, and the password MirrorMaker uses to log in to the mirror. |
| `jmx-sage-kafka.yml`, `jmx-mirrormaker.yml` | Rules that turn each process's JMX metrics into Prometheus metrics. |
| `mirror-explorer.ipynb` | A Jupyter notebook for looking at what's on the mirror (see [Exploring the mirror in a notebook](#exploring-the-mirror-in-a-notebook)). |

## 1. Before the first start

`mm2.properties` contains the current configuration to target a NEON Kafka server that is accessible to a Sage Thor node.
The secrets file is where you define the `primary.bootstrap.servers` via `NEON_BOOTSTRAP_SERVER` and also the credentials for the server
since SASL/SCRAM usernames and passwords are also being used, plus `MIRROR_MM2_PASSWORD`, the password MirrorMaker uses to log in to the mirror.
For the podman deployment that's `podman/mirror-secrets.yaml`, a `ConfigMap`. For Kubernetes, `k8s/mirror-secrets.yaml` holds the same contents 
as a `Secret` (`kind: Secret`, `stringData:` in place of `data:`). Both files are in the `.gitignore` file.

## 2. Optional settings

These are set in the `env:` list of the `sage-kafka` container, in `podman/sage-kafka.yaml` and `k8s/sage-kafka.yaml`.

| Variable | Default | Purpose |
|---|---|---|
| `MIRROR_ADVERTISED_HOST` | `localhost` | Hostname or IP that clients outside the pod use to reach the mirror. Set it to the host's name (Podman) or the `kafka-mirror` Service's address (Kubernetes) if those clients run on other machines. |
| `MIRROR_EXTERNAL_PORT` | `9094` | Port for those clients. It must match the published port: the `hostPort` (Podman) or the Service port (Kubernetes). |
| `MIRROR_CLUSTER_ID` | a fixed ID | Only used the first time the broker formats its storage. Changing it later requires wiping the volume. |

Both containers run the image built from the `Dockerfile`, which is based on `apache/kafka:4.3.1`. To use another base image, 
pass `--build-arg KAFKA_IMAGE=...` to `podman build`.

`log.retention.hours` in `sage-kafka.properties` (168 hours) applies to mirrored topics that don't set their own retention on the source, 
because MM2 copies per-topic settings but not the source broker's defaults.

## 3. Start it

```bash
#Podman (Sage Thor)
podman build -t localhost/kafka-mirror/kafka-jmx:local .
podman kube play podman/sage-kafka-pvc.yaml
podman kube play --configmap mirror-secrets.yaml sage-kafka.yaml
podman kube play --replace --configmap mirror-secrets.yaml sage-kafka.yaml

#Kubernetes (k3s on Sage Thor)
docker build -t 10.31.81.1:5000/local/kafka-jmx:latest .
docker push 10.31.81.1:5000/local/kafka-jmx:latest
kubectl apply -f k8s/mirror-secrets.yaml
kubectl apply -k .

#Generate files to check if everything works
kubectl kustomize . > k8s/sage-neon-broker.yaml

```

On Kubernetes, set `MIRROR_ADVERTISED_HOST` in `k8s/sage-kafka.yaml` to the address clients use to reach the
`kafka-mirror` Service. To run the `podman exec` commands below, replace
`podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka` with `kubectl exec kafka-mirror-0 -c sage-kafka -- env KAFKA_OPTS=`.

MM2 can take a minute or two after startup before records start flowing. On the first run it copies everything the source still retains for 
the selected topics, so large topics take a while. 

To copy only new data instead, uncomment `primary.consumer.auto.offset.reset = latest` in `mm2.properties` before the first start.

## 4. Check that it's working

Admin commands run inside the broker container use the loopback-only `LOCAL` listener on `127.0.0.1:9095`, and log in as the internal 
`admin` user with `/tmp/admin.properties`:

```bash
# Topics on the mirror
podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9095 --command-config /tmp/admin.properties --list

# Read a few records from a mirrored topic
podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 127.0.0.1:9095 --command-config /tmp/admin.properties \
  --topic reading.sensor.csat3b --max-messages 5

```

Offsets on the mirror won't equal the source's, so compare record counts per partition (end minus start) rather than raw offsets. Besides your topics you'll see `heartbeats`, `primary.checkpoints.internal`, and several `mm2-...internal` topics.

## Exploring the mirror in a notebook

`mirror-explorer.ipynb` now lives in its seperate repository with the neon_sage_edge client: [notebook](https://github.com/sagecontinuum/neon_sage_edge/blob/main/notebooks/mirror-explorer.ipynb)

## Users and authentication

The broker has four listeners. 

- `INTERNAL` (port 9092, for containers in the pod, which is what MM2 uses and the admin account) and uses SASL/SCRAM-SHA-512. 
- `EXTERNAL` (port 9094, published for everything outside the pod) does not require usernames/passwords and are all monitor in the username ANONYMOUS when looking at the grafana dashboard.
- `LOCAL` (127.0.0.1:9095) and `CONTROLLER` (127.0.0.1:9093) only accessible to the admin and inter-broker traffic, but they only listen on the container's loopback interface, so only processes
inside the container can use them. 

The Kafka mirror has the `EXTERNAL` port open to anyone to use, but this is kept just in case we
decide to add back SASL/SCRAM-SHA-512 in the future:

```bash
# Add a user (or reset its password)
podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 127.0.0.1:9095 --command-config /tmp/admin.properties --alter \
  --entity-type users --entity-name sage --add-config 'SCRAM-SHA-512=[password=sagepass]'

# List users
podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 127.0.0.1:9095 --command-config /tmp/admin.properties \
   --describe --entity-type users

# Remove a user's password, so it can no longer log in
podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 127.0.0.1:9095 --alter --command-config /tmp/admin.properties \
  --entity-type users --entity-name sage --delete-config 'SCRAM-SHA-512'
```

Clients outside the pod connect to `localhost:9094` (or `MIRROR_ADVERTISED_HOST:MIRROR_EXTERNAL_PORT`) with 
settings like these, passed to the Kafka CLI tools with `--command-config client.properties`:

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="sage" password="sagepass";
```

The Kafka mirror has an `authorizer` set and all newly created users only have READ-only access.

To confirm that READ-only access is working:
```bash
podman exec -i kafka-mirror-sage-kafka sh -c 'cat > /tmp/notebook.properties' <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="sage" password="sagepass";
EOF

podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --command-config /tmp/notebook.properties \
  --create --topic reading.notebook-made --partitions 1 --replication-factor 1
```
This should return the following:
```bash
Error while executing topic command : Authorization failed.
[2026-10-08 21:06:32,760] ERROR org.apache.kafka.common.errors.TopicAuthorizationException: Authorization failed.
 (org.apache.kafka.tools.TopicCommand)
```
Note that users is not allowed to write to an existing topic.

To allow a user permissions to write to a new topic called `my-app-output`, here is an example:
```bash
podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server 127.0.0.1:9095" --add --allow-principal User:sage \
  --operation Write --operation Describe --topic my-app-output

```

## Metrics

Each container runs the Prometheus JMX exporter as a Java agent. The broker's metrics are at `http://localhost:7071/metrics` and MirrorMaker's at `http://localhost:7072/metrics`. A Prometheus scrape config for them:

```yaml
scrape_configs:
  - job_name: kafka-mirror
    static_configs:
      - targets: ["<host>:7071"]
  - job_name: mirrormaker
    static_configs:
      - targets: ["<host>:7072"]
```

The broker exports the usual Kafka metrics, such as bytes in and out per topic (`kafka_server_brokertopicmetrics_bytesin_total{topic="..."}`), under-replicated partitions, request latencies, and KRaft state. MirrorMaker exports replication latency and record age per mirrored partition (`kafka_mirror_source_replication_latency_ms`, `kafka_mirror_source_record_age_ms`), how far its consumers are behind the source (`kafka_mirror_consumer_records_lag_max`), checkpoint latency per consumer group, and the status of its connectors and tasks (`kafka_mirror_task_status{status="failed"}` is worth an alert).

### Per-user metrics

Kafka only records per-user usage once at least one client quota exists. This one-time command sets a default quota of 1 GiB/s per user, far above what a single broker like this will see, which switches the metrics on without throttling anyone:

```bash
podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 127.0.0.1:9095 --command-config /tmp/admin.properties --alter --entity-type users --entity-default --add-config 'producer_byte_rate=1073741824,consumer_byte_rate=1073741824'
```

From then on the broker exports `kafka_user_bytes_per_second{request="Produce|Fetch", user="..."}` and `kafka_user_throttle_time_ms` for every user that's active. MM2 shows up as `user="mm2"`, and the password-free admin tools as `user="ANONYMOUS"`. The rate covers roughly the last ten seconds, so for usage over time let Prometheus integrate it; for example, approximate bytes each user produced over the last hour:

```promql
avg_over_time(kafka_user_bytes_per_second{request="Produce"}[1h]) * 3600
```

To see it with your own eyes, create a user and a scratch topic, then produce as that user for 30 seconds:

```bash
podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 127.0.0.1:9095 --alter --entity-type users --entity-name sage \
  --add-config 'SCRAM-SHA-512=[password=sagepass]'

podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9095 --create --topic sage-demo --partitions 1

# sage's client settings, written inside the container
podman exec kafka-mirror-sage-kafka sh -c 'cat > /tmp/sage.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="sage" password="sagepass";
EOF'

# About 2 MB/s as sage for 30 seconds, through the authenticated INTERNAL listener
podman exec kafka-mirror-sage-kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
  --bootstrap-server 127.0.0.1:9092 --command-config /tmp/sage.properties \
  --topic sage-demo --num-records 300000 --record-size 200 --throughput 10000
```

While it runs, `curl -s localhost:7071/metrics | grep kafka_user_bytes_per_second` in another terminal shows a line for `user="sage"`. Delete the scratch topic afterwards with `kafka-topics.sh --bootstrap-server 127.0.0.1:9095 --delete --topic sage-demo`, run the same way as the other admin commands.
