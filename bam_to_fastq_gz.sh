#!/usr/bin/env bash

set -e
set -u

DEMUX_BAM_FNAME="$1"
FASTQ_GZ_DIV_FNAME="$2"

samtools fastq "$DEMUX_BAM_FNAME" | gzip > "$FASTQ_GZ_DIV_FNAME"
