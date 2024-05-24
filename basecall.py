#!/usr/bin/env python3
import os
import sys
import shutil
import subprocess

# Prereq: mount-s3 nao-restricted ~/s3-mnt/nao-restricted/
# Usage: /basecall.py "dna_r10.4.1_e8.2_400bps_hac@v4.1.0" SQK-NBD114-24 \
#   ~/s3-mnt/nao-restricted/NAO-ONT-20240519-practice/fast5/ \
#   NAO-ONT-20240519-practice

model, kit, s3_in_dir, out_prefix = sys.argv[1:]

BATCH_SIZE=1024**3  # 1GiB

def batch_input_files():
    batches = []
    current_batch = []
    current_batch_size = 0

    for fname in os.listdir(s3_in_dir):
        path = os.path.join(s3_in_dir, fname)
        size = os.path.getsize(path)

        if current_batch_size + size > BATCH_SIZE:
            yield current_batch
            current_batch = []
            current_batch_size = 0

        current_batch.append(path)
        current_batch_size += size

    if current_batch:
        yield current_batch

for i, batch in enumerate(batch_input_files()):
    print("Processing batch %s" % i)
    batch_dir = "batch-fast5-%i" % i
    out_fname = "%s-%s.bam" % (out_prefix, i)

    if not os.path.exists(batch_dir):
        os.mkdir(batch_dir)
        for fname in batch:
            shutil.copy(fname, batch_dir)

    subprocess.check_call([
        "./basecall.sh", kit, model, batch_dir, out_fname])
        
    shutil.rmtree(batch_dir)



    
