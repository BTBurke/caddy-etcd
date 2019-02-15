#!/bin/bash

docker run \
  --rm \
  -p 2379:2379 \
  -p 4001:4001 \
  --name etcd \
  -v /usr/share/ca-certificates/:/etc/ssl/certs \
  quay.io/coreos/etcd:latest \
  etcd --listen-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001 --advertise-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001