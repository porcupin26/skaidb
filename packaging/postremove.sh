#!/bin/sh
# Post-remove for the skaidb .deb / .rpm: reload systemd after the unit file is
# gone. The data dir (/var/lib/skaidb) and the skaidb account are deliberately
# left in place so an uninstall never destroys data; remove them by hand if you
# really mean to.
set -e

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi

exit 0
