#!/usr/bin/env bash
mkdir -p package
pip install -r requirements-runtime.txt --target package
cd package
zip -rA ../../../../preingest-pa-importer .
cd ..
zip -rA ../../../preingest-pa-importer lambda_function.py
cd ../../../
zip -rA preingest-pa-importer common/