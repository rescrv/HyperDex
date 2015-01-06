#!/bin/sh

sudo apt-get install -y build-essential libgoogle-glog-dev \
    libpopt-dev make pkg-config wget

wget https://github.com/jedisct1/libsodium/releases/download/1.0.1/libsodium-1.0.1.tar.gz
tar xzvf libsodium-1.0.1.tar.gz
cd libsodium-1.0.1
./configure
make
sudo make install
