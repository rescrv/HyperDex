#!/bin/sh

echo >> /etc/yum.conf << EOF
[hyperdex]
name=hyperdex
baseurl=http://centos.hyperdex.org/base/$basearch/$releasever
enabled=1
EOF

yum install -y hyperdex
