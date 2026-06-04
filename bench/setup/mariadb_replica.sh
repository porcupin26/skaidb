#!/usr/bin/env bash
# Configure a MariaDB semi-sync replica via GTID. Run on each replica.
# Env: PRIMARY_IP (required), REPL_PASS (required), SERVER_ID (2)
set -e
cat > /etc/mysql/mariadb.conf.d/zz-repl.cnf <<CFG
[mysqld]
bind-address = 0.0.0.0
server_id = ${SERVER_ID:-2}
log_bin = mariadb-bin
log_basename = mariadb
binlog_format = ROW
relay_log = relay-bin
read_only = ON
innodb_buffer_pool_size = 128M
max_connections = 200
gtid_strict_mode = ON
CFG
systemctl restart mariadb; sleep 3
mariadb -e "INSTALL SONAME 'semisync_slave';" 2>/dev/null || true
mariadb -e "SET GLOBAL rpl_semi_sync_slave_enabled=ON;"
mariadb -e "STOP SLAVE; RESET SLAVE; CHANGE MASTER TO MASTER_HOST='${PRIMARY_IP}', MASTER_USER='repl', MASTER_PASSWORD='${REPL_PASS}', MASTER_USE_GTID=slave_pos; START SLAVE;"
sleep 3
mariadb -e "SHOW SLAVE STATUS\G" | grep -E "Slave_IO_Running:|Slave_SQL_Running:"
