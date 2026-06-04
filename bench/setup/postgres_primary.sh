#!/usr/bin/env bash
# Configure a PostgreSQL primary for streaming replication. Run on the primary.
# Env: REPL_PASS (required), APP_USER (skaidb), APP_PASS (required),
#      SUBNET (192.168.0.0/16), PGVER (15)
set -e
PGVER=${PGVER:-15}
CONF=/etc/postgresql/${PGVER}/main
mkdir -p "$CONF/conf.d"
cat > "$CONF/conf.d/repl.conf" <<CFG
listen_addresses = '*'
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
synchronous_commit = on
shared_buffers = 128MB
max_connections = 100
CFG
grep -q "${SUBNET:-192.168.0.0/16}" "$CONF/pg_hba.conf" || cat >> "$CONF/pg_hba.conf" <<HBA
host all all ${SUBNET:-192.168.0.0/16} md5
host replication replicator ${SUBNET:-192.168.0.0/16} md5
HBA
systemctl restart postgresql; sleep 3
psql() { runuser -u postgres -- psql "$@"; }
psql -c "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '${REPL_PASS}';"
psql -c "CREATE ROLE ${APP_USER:-skaidb} WITH LOGIN PASSWORD '${APP_PASS}' SUPERUSER;"
psql -tc "SELECT 1 FROM pg_database WHERE datname='bench'" | grep -q 1 \
  || runuser -u postgres -- createdb -O "${APP_USER:-skaidb}" bench
psql -d bench -c "CREATE TABLE IF NOT EXISTS bench (id bigint PRIMARY KEY, v text); ALTER TABLE bench OWNER TO ${APP_USER:-skaidb};"
echo "primary ready"

# After the standby(s) join, set write durability and reload:
#   ALTER SYSTEM SET synchronous_standby_names='';                          -- C2: primary only
#   ALTER SYSTEM SET synchronous_standby_names='standby1';                  -- C1: both (2 nodes)
#   ALTER SYSTEM SET synchronous_standby_names='FIRST 2 (standby1,standby2)'; -- C3: all 3
#   ALTER SYSTEM SET synchronous_standby_names='ANY 1 (standby1,standby2)';   -- C4: quorum (2 of 3)
#   SELECT pg_reload_conf();
