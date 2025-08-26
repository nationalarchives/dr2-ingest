#!/usr/bin/env bash
mkdir -p package
pip install -r requirements-runtime.txt --target package
cd package
zip -rA ../../../../preingest-tdr-importer .
cd ..
zip -rA ../../../preingest-tdr-importer lambda_function.py
cd ../../../
zip -rA preingest-tdr-importer common/