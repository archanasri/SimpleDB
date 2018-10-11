#!/bin/bash

BASEDIR="$(cd "`dirname "$0"`" && pwd)"

if [ $# -lt 1 ]; then
    echo "usage: <previous lab dir>"
    exit 1
fi

cd "$BASEDIR"
cp -r * "$1/"
rm -f "$1/copy-supplement-files.sh"
