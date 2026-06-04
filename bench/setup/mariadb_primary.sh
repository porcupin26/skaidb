#!/usr/bin/env bash
# Configure a MariaDB primary with binlog + semi-sync. Run on the primary.
# Env: REPL_PASS (required), APP_USER (skaidb), APP_PASS (required)
#
# Note: MariaDB semi-sync acks after the *first* replica responds and has no
# "wait for N replicas" knob, so it expresses primary-only (semi-sync OFF) and
# ~quorum (semi-sync ON), but not strict all-N durability.
set -e
cat > /etc/mysql/mariadb.conf.d/zz-repl.cnf <<CFG
[mysqld]
bind-address = 0.0.0.0
server_id = 1
log_bin = mariadb-bin
log_basename = mariadb
binlog_format = ROW
innodb_buffer_pool_size = 128M
innodb_flush_log_at_trx_commit = 1
sync_binlog = 1
max_connections = 200
gtid_strict_mode = ON
CFG
systemctl restart mariadb; sleep 3
mariadb -e "CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY '${REPL_PASS}'; GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';"
mariadb -e "CREATE USER IF NOT EXISTS '${APP_USER:-skaidb}'@'%' IDENTIFIED BY '${APP_PASS}'; GRANT ALL PRIVILEGES ON *.* TO '${APP_USER:-skaidb}'@'%' WITH GRANT OPTION; FLUSH PRIVILEGES;"
mariadb -e "CREATE DATABASE IF NOT EXISTS bench; CREATE TABLE IF NOT EXISTS bench.bench (id bigint PRIMARY KEY, v text) ENGINE=InnoDB;"
mariadb -e "INSTALL SONAME 'semisync_master';" 2>/dev/null || true
# Toggle write durability:
#   SET GLOBAL rpl_semi_sync_master_enabled=ON;   -- wait for one replica (~quorum)
#   SET GLOBAL rpl_semi_sync_master_enabled=OFF;  -- primary only (async)
mariadb -e "SET GLOBAL rpl_semi_sync_master_enabled=ON; SET GLOBAL rpl_semi_sync_master_timeout=10000;"
echo "primary ready"
