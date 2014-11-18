#!/bin/sh

tar xzvf hyperdex-1.5.0-linux-amd64.tar.gz -C /usr/local
export PATH=/usr/local/hyperdex/bin:${PATH}
hyperdex --version
