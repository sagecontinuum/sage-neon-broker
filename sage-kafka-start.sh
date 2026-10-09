#!/usr/bin/env bash
# =============================================================================
# Starts the mirror broker (sage-kafka):
#   1. generates a password for the internal "admin" user, which the broker's
#      loopback-only LOCAL and CONTROLLER listeners require. It's new at every
#      start, and admin commands log in with /tmp/admin.properties:
#        podman exec -e KAFKA_OPTS= kafka-mirror-sage-kafka /opt/kafka/bin/kafka-topics.sh \
#          --bootstrap-server 127.0.0.1:9095 --command-config /tmp/admin.properties --list
#   2. formats its storage on first start, creating the "mm2" SCRAM user
#   3. applies the access rules in kafka-acls.conf in the background, once the
#      broker answers
#   4. runs the broker, which takes over this process
#
# Mounted into the sage-kafka container by podman/kafka-mirror-pod.yaml. Settings
# come from the container's environment:
#   MIRROR_CLUSTER_ID       cluster ID used when formatting the storage
#   MIRROR_MM2_PASSWORD     password for the "mm2" user (first start only)
#   MIRROR_ADVERTISED_HOST  host clients outside the pod use to reach the broker
#   MIRROR_EXTERNAL_PORT    port clients outside the pod use (9094)
#   KAFKA_OPTS              attaches the Prometheus exporter to the broker
# =============================================================================
set -eu

KAFKA_BIN=/opt/kafka/bin
CONFIG=/etc/kafka/mirror/server.properties   # mounted settings, without passwords
RUNTIME_CONFIG=/tmp/server.properties        # the same plus the generated admin login
ADMIN_CONFIG=/tmp/admin.properties           # client settings for admin commands
ACL_RULES=/etc/kafka/mirror/acls.conf
LOCAL_BOOTSTRAP=127.0.0.1:9095               # loopback-only listener for admin tasks

# A fresh password for the internal "admin" user. Only this container needs it:
# the broker uses it on its own loopback connections, and admin commands read it
# from ADMIN_CONFIG. Both files are readable by this container's user only.
write_admin_login() {
  local password jaas
  password=$(tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32)
  jaas="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"$password\""
  (
    umask 077
    {
      cat "$CONFIG"
      echo
      echo "# --- Added by sage-kafka-start.sh: the internal admin login -----------------"
      echo "listener.name.local.plain.sasl.jaas.config=$jaas user_admin=\"$password\";"
      echo "listener.name.controller.plain.sasl.jaas.config=$jaas user_admin=\"$password\";"
    } > "$RUNTIME_CONFIG"
    {
      echo "security.protocol=SASL_PLAINTEXT"
      echo "sasl.mechanism=PLAIN"
      echo "sasl.jaas.config=$jaas;"
    } > "$ADMIN_CONFIG"
  )
}

# First start only (--ignore-formatted makes later starts skip it). KAFKA_OPTS is
# cleared for this command so it doesn't start an exporter of its own.
format_storage() {
  KAFKA_OPTS='' "$KAFKA_BIN/kafka-storage.sh" format --ignore-formatted \
    --cluster-id "$MIRROR_CLUSTER_ID" \
    --config "$RUNTIME_CONFIG" \
    --add-scram "SCRAM-SHA-512=[name=mm2,password=$MIRROR_MM2_PASSWORD]"
}

acl_log() {
  echo "[kafka-acls] $*"
}

# One rule from the rules file, with the usual kafka-acls.sh options. Retried a
# few times, since the broker can need a moment after it first answers.
acl() {
  for _ in 1 2 3 4 5; do
    if "$KAFKA_BIN/kafka-acls.sh" --bootstrap-server "$LOCAL_BOOTSTRAP" --command-config "$ADMIN_CONFIG" "$@" > /tmp/acl.out 2>&1; then
      acl_log "applied: $*"
      return 0
    fi
    sleep 3
  done
  acl_log "FAILED: $*"
  sed 's/^/[kafka-acls]   /' /tmp/acl.out
}

# Waits until the broker answers, then applies every rule in the rules file.
# Runs in the background; its messages start with [kafka-acls].
apply_acls() {
  set +e                # one failing rule must not stop the others
  export KAFKA_OPTS=''  # Kafka tools here must not try to open a second exporter
  until "$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server "$LOCAL_BOOTSTRAP" \
        --command-config "$ADMIN_CONFIG" > /dev/null 2>&1; do
    sleep 3
  done
  if [ ! -f "$ACL_RULES" ]; then
    acl_log "No rules file at $ACL_RULES; skipping."
    return 0
  fi
  # shellcheck source=/dev/null
  . "$ACL_RULES"
  acl_log "Access rules applied."
}

write_admin_login
format_storage
apply_acls &

# INTERNAL is advertised as localhost, since MirrorMaker shares this pod's network.
exec "$KAFKA_BIN/kafka-server-start.sh" "$RUNTIME_CONFIG" \
  --override "advertised.listeners=INTERNAL://localhost:9092,EXTERNAL://${MIRROR_ADVERTISED_HOST}:${MIRROR_EXTERNAL_PORT},LOCAL://127.0.0.1:9095"
