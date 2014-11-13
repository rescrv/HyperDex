#!/bin/sh

set -e

cat >> /etc/yum.conf << EOF
[hyperdex]
name=hyperdex
baseurl=http://centos.hyperdex.org/base/\$basearch/\$releasever
enabled=1
gpgcheck=1
EOF
rpm --import http://centos.hyperdex.org/hyperdex.gpg.key
yum install -y epel-release
yum install -y hyperdex
