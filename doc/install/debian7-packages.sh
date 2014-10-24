#!/bin/sh

set -e

wget -O - http://debian.hyperdex.org/hyperdex.gpg.key | apt-key add -
cat >> /etc/apt/sources.list.d/hyperdex.list << EOF
deb [arch=amd64] http://debian.hyperdex.org wheezy main
EOF
apt-get update
apt-get install -y hyperdex
