#!/usr/bin/env bash
# Install MongoDB and configure one replica-set member. Run on every node.
# Env: MONGO_VER (default 7.0), MONGO_RS (default rs0)
#
# MongoDB 8.0 requires a Linux kernel < 6.19 (8.0.15+ refuse to start on newer
# kernels; 8.0.0 segfaults). MongoDB 7.0 has no such restriction.
set -e
VER=${MONGO_VER:-7.0}
RS=${MONGO_RS:-rs0}
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq gnupg curl ca-certificates >/dev/null 2>&1
curl -fsSL "https://www.mongodb.org/static/pgp/server-${VER}.asc" \
  | gpg -o "/usr/share/keyrings/mongodb-${VER}.gpg" --dearmor --yes
echo "deb [ signed-by=/usr/share/keyrings/mongodb-${VER}.gpg ] http://repo.mongodb.org/apt/debian bookworm/mongodb-org/${VER} main" \
  > "/etc/apt/sources.list.d/mongodb-${VER}.list"
apt-get update -qq
apt-get install -y -qq mongodb-org >/dev/null 2>&1
cat > /etc/mongod.conf <<CFG
storage: { dbPath: /var/lib/mongodb, wiredTiger: { engineConfig: { cacheSizeGB: 0.25 } } }
systemLog: { destination: file, logAppend: true, path: /var/log/mongodb/mongod.log }
net: { port: 27017, bindIp: 0.0.0.0 }
replication: { replSetName: ${RS} }
CFG
systemctl enable --now mongod
echo "mongod $(mongod --version | head -1) active=$(systemctl is-active mongod)"

# Then, from ONE node, initiate and grow the set:
#   mongosh --eval 'rs.initiate({_id:"'"$RS"'", members:[
#     {_id:0,host:"NODE1:27017"},{_id:1,host:"NODE2:27017"}]})'
#   mongosh "mongodb://NODE1:27017/?replicaSet=$RS" --eval 'rs.add("NODE3:27017")'
#
# Write durability is per-operation on the client (MONGO_W): w=1 primary-only,
# w=majority quorum, w=3 all-3.
