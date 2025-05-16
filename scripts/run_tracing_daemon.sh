#!/bin/bash

# We assume traced and all is in a path, like export PATH="WHATEVER_/perfetto/out/linux/:$PATH"
set -e



pkill -9 -f traced || true
pkill -9 -f traced_probes || true

if [ ! -w /sys/kernel/tracing ]; then
    echo "tracefs not accessible, try sudo chown -R $USER /sys/kernel/tracing"
    sudo chown -R "$USER" /sys/kernel/tracing
fi

echo 0 > /sys/kernel/tracing/tracing_on

traced &
traced_probes &
