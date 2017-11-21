#!/bin/bash

export RELX_REPLACE_OS_VARS=true

PLATFORM_DATA_DIR="data/${1}" RING_STATE_DIR="data/ring/${1}" HANDOFF_PORT=8${1}99 PB_PORT=8${1}87 PUBSUB_PORT=8${1}86 LOGREADER_PORT=8${1}85 INSTANCE_NAME=antidote${1} PB_IP=127.0.0.1 COOKIE=antidote _build/default/rel/antidote${1}/bin/env remote_console
