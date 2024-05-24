#!/usr/bin/env bash

set -e
set -u

KIT="$1"
MODEL="$2"
INDIR="$3"
OUTFNAME="$4"

~/dorado-0.6.1-linux-x64/bin/dorado \
    basecaller \
    --kit-name "$KIT" \
    "$MODEL" \
    "$INDIR" > "$OUTFNAME"
