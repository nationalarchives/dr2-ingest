#!/usr/bin/env bash
mkdir -p package
pip install -r requirements-runtime.txt --target package
cd package
zip -rA ../../../../copy-files-from-tdr .
cd ..
zip -rA ../../../copy-files-from-tdr lambda_function.py
cd ../../../
zip -rA copy-files-from-tdr common/