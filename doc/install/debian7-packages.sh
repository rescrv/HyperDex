#!/bin/sh

set -e

wget -O - http://debian.hyperdex.org/hyperdex.gpg.key | apt-key add -
wget -O /etc/apt/sources.list.d/hyperdex.list http://debian.hyperdex.org/wheezy.list
apt-get update
apt-get install -y hyperdex
