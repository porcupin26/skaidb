#!/bin/sh
# Post-install for the skaidb .deb / .rpm: create the service account, lock down
# the data + config dirs, and register the systemd unit. Idempotent.
set -e

# Dedicated, unprivileged system account.
if ! getent group skaidb >/dev/null 2>&1; then
    groupadd --system skaidb
fi
if ! getent passwd skaidb >/dev/null 2>&1; then
    useradd --system --gid skaidb --home-dir /var/lib/skaidb \
        --shell /usr/sbin/nologin --comment "skaidb server" skaidb
fi

# Data directory, owned by the service account.
mkdir -p /var/lib/skaidb
chown skaidb:skaidb /var/lib/skaidb
chmod 0750 /var/lib/skaidb

# Log directory, owned by the service account. The shipped config writes audit
# logs here (observability.log_file); under systemd LogsDirectory= also manages
# it, but create it now so a direct run or a non-systemd host has it too.
mkdir -p /var/log/skaidb
chown skaidb:skaidb /var/log/skaidb
chmod 0750 /var/log/skaidb

# Config readable by the service but not world-readable (it may hold a
# password). Group-writable so runtime `config set` can persist changes
# (the unit opens /etc/skaidb via ReadWritePaths for the same reason).
if [ -d /etc/skaidb ]; then
    chown -R root:skaidb /etc/skaidb
    chmod 0770 /etc/skaidb
    [ -f /etc/skaidb/skaidb.toml ] && chmod 0660 /etc/skaidb/skaidb.toml
fi

# Is this an upgrade or a fresh install? deb and rpm signal it differently:
#   deb: postinst is run as `configure <old-version>`; a non-empty old version
#        means an upgrade ($2 is empty on first install).
#   rpm: %post receives the number of this package left installed afterwards;
#        1 == first install, >=2 == upgrade.
upgrade=0
if [ "$1" = configure ] && [ -n "$2" ]; then
    upgrade=1
elif [ "$1" -ge 2 ] 2>/dev/null; then
    upgrade=1
fi

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    if [ "$upgrade" -eq 1 ]; then
        # Restart so the running daemon picks up the new binary — but only if it
        # is currently active. `try-restart` won't start a service the operator
        # has stopped or never enabled. (In a multi-node cluster, upgrade one
        # host at a time so the nodes don't all bounce at once.)
        systemctl try-restart skaidb.service >/dev/null 2>&1 || true
    else
        # Fresh install: register the unit per distro policy without starting it
        # — the operator edits the config, then enables it.
        systemctl preset skaidb.service >/dev/null 2>&1 || true
        cat <<'EOF'
skaidb installed. Next steps:
  1. Edit /etc/skaidb/skaidb.toml (or set SKAIDB_* in /etc/default/skaidb).
  2. sudo systemctl enable --now skaidb
  3. systemctl status skaidb   # logs: journalctl -u skaidb -f
EOF
    fi
fi

exit 0
