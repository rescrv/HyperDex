#!/bin/sh

set -e

wget -O - http://ubuntu.hyperdex.org/hyperdex.gpg.key | apt-key add -
cat >> /etc/apt/sources.list.d/hyperdex.list << EOF
deb [arch=amd64] http://ubuntu.hyperdex.org precise main
EOF
apt-get update
apt-get install -y hyperdex
