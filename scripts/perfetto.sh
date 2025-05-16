#!/bin/bash

# We assume traced and all is in a path, like export PATH="WHATEVER_/perfetto/out/linux/:$PATH"
set -e

perfetto --txt -c logging_tracing/configs/perfetto.cfg -o "system_trace_$(date +"%Y-%m-%d_%H-%M-%S").txt"