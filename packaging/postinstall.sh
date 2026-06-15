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

# Config readable by the service but not world-readable (it may hold a password).
if [ -d /etc/skaidb ]; then
    chown -R root:skaidb /etc/skaidb
    chmod 0750 /etc/skaidb
    [ -f /etc/skaidb/skaidb.toml ] && chmod 0640 /etc/skaidb/skaidb.toml
fi

# Register the unit. `preset` honours distro enable/disable policy without
# starting the service — the operator edits the config, then enables it.
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    systemctl preset skaidb.service >/dev/null 2>&1 || true
    cat <<'EOF'
skaidb installed. Next steps:
  1. Edit /etc/skaidb/skaidb.toml (or set SKAIDB_* in /etc/default/skaidb).
  2. sudo systemctl enable --now skaidb
  3. systemctl status skaidb   # logs: journalctl -u skaidb -f
EOF
fi

exit 0
