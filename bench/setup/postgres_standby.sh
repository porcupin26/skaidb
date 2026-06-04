#!/usr/bin/env bash
# Provision a PostgreSQL hot standby from a primary base backup. Run on the standby.
# Env: PRIMARY_IP (required), REPL_PASS (required), APP_NAME (standby1),
#      SUBNET (192.168.0.0/16), PGVER (15)
set -e
PGVER=${PGVER:-15}
CONF=/etc/postgresql/${PGVER}/main
DATA=/var/lib/postgresql/${PGVER}/main
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
systemctl stop postgresql
runuser -u postgres -- bash -c "echo '${PRIMARY_IP}:5432:replication:replicator:${REPL_PASS}' > /var/lib/postgresql/.pgpass; chmod 600 /var/lib/postgresql/.pgpass"
rm -rf "$DATA"; mkdir -p "$DATA"; chown postgres:postgres "$DATA"; chmod 700 "$DATA"
runuser -u postgres -- env PGPASSWORD="${REPL_PASS}" \
  pg_basebackup -h "${PRIMARY_IP}" -U replicator -D "$DATA" -X stream -R \
  -d "application_name=${APP_NAME:-standby1}"
systemctl start postgresql; sleep 3
echo -n "in_recovery="; runuser -u postgres -- psql -tAc "SELECT pg_is_in_recovery();"
