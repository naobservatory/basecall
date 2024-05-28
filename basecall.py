#!/usr/bin/env python3
import os
import sys
import shutil
import subprocess

# Usage: ./basecall.py ~/dna_r10.4.1_e8.2_400bps_hac@v4.1.0 SQK-NBD114-24 \
#   NAO-ONT-20240519-practice

BATCH_SIZE=1024**3  # 1GiB

model_path, kit, bioproject = sys.argv[1:]

if not os.path.exists(model_path):
    raise Exception("Model not present: %r" % model_path)

WORK_DIR=os.path.join(os.path.expanduser("~/basecall-work"), bioproject)
os.makedirs(WORK_DIR, exist_ok=True)

S3_DIR=os.path.expanduser("~/s3-mnt/nao-restricted/")
def s3_mounted():
    return os.path.exists(os.path.join(S3_DIR, "nao-restricted-exists"))

if not s3_mounted():
    subprocess.check_call(["mount-s3", "nao-restricted", S3_DIR])
assert s3_mounted()

s3_in_dir = os.path.join(S3_DIR, bioproject, "fast5")
s3_out_dir = os.path.join(S3_DIR, bioproject, "raw")

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

# TODO: this could be made faster by parallelizing: we could be simultaneously
# copying down files for batch 3, basecalling batch 2, and demultiplexing batch
# 1.
for i, batch in enumerate(batch_input_files()):
    print("Processing batch %s..." % i)

    bam_fname = os.path.join(WORK_DIR, "%s.bam" % i)
    if os.path.exists(bam_fname):
        print("Skipping basecalling because already complete.")
    else:
        print("Basecalling...")
        batch_dir = os.path.join(WORK_DIR, "batch-fast5-%i" % i)

        if not os.path.exists(batch_dir):
            os.mkdir(batch_dir)
            print("Copying down files to basecall...")
            for fname in batch:
                shutil.copy(fname, batch_dir)

        subprocess.check_call([
            "./basecall.sh", kit, model_path, batch_dir, bam_fname])

        shutil.rmtree(batch_dir)

    demux_dir = os.path.join(WORK_DIR, "demux-%i" % i)
    if os.path.exists(demux_dir):
        print("Skipping demultiplexing because already exists.")
    else:
        print("Demultiplexing...")
        subprocess.check_call([
            os.path.expanduser("~/dorado-0.6.1-linux-x64/bin/dorado"),
            "demux",
            "--output-dir", demux_dir,
            "--kit-name", kit,
            bam_fname])

    for demux_bam_leaf in os.listdir(demux_dir):
        assert demux_bam_leaf.endswith(".bam")
        barcode = demux_bam_leaf.replace(
            "%s_barcode" % kit, "").replace(".bam", "")
        demux_bam_fname = os.path.join(demux_dir, demux_bam_leaf)
        fastq_gz_div_fname = os.path.join(
            WORK_DIR,
            "%s-%s-div%04d.fastq.gz" % (bioproject, barcode, i))
        subprocess.check_call([
            "./bam_to_fastq_gz.sh", demux_bam_fname, fastq_gz_div_fname])

        subprocess.check_call([
            "aws", "s3", "cp", fastq_gz_div_fname,
            "s3://nao-restricted/%s/raw/" % bioproject])

        os.remove(fastq_gz_div_fname)

    shutil.rmtree(demux_dir)
    
