#!/bin/sh
# Pre-remove for the skaidb .deb / .rpm: stop and disable the service on an
# actual removal (not on an upgrade). The first arg distinguishes the two:
#   deb: "remove" / "purge" on removal, "upgrade" on upgrade
#   rpm: "0" on removal, "1" on upgrade
set -e

case "$1" in
    remove|purge|0)
        if command -v systemctl >/dev/null 2>&1; then
            systemctl stop skaidb.service >/dev/null 2>&1 || true
            systemctl disable skaidb.service >/dev/null 2>&1 || true
        fi
        ;;
esac

exit 0
