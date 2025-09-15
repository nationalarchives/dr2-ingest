#!/usr/bin/env bash
mkdir -p package
cd package
zip -rA ../../../../ingest-metric-collector .
cd ..
zip -rA ../../../ingest-metric-collector ingest_metric_collector.py
