#!/bin/sh

set -e

cat >> /etc/yum.conf << EOF
[hyperdex]
name=hyperdex
baseurl=http://fedora.hyperdex.org/base/\$basearch/\$releasever
enabled=1
gpgcheck=1
EOF
rpm --import http://fedora.hyperdex.org/hyperdex.gpg.key
yum install -y hyperdex
