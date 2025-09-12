#!/usr/bin/env bash
mkdir -p package
cd package
zip -rA ../../../../ingest-metric-collector .
cd ..
zip -rA ../../../ingest-metric-collector ingest-metric-collector.py
cd ../../../
zip -rA preingest-tdr-importer common/