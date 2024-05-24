#!/usr/bin/env python3
import os
import sys
import shutil
import subprocess

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



    
