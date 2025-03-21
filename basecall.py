#!/usr/bin/env python3
import os
import sys
import shutil
import subprocess
import argparse

# Usage: ./basecall.py --kit SQK-NBD114-24 NAO-ONT-20240519-practice

BATCH_SIZE=1024**3  # 1GiB

parser = argparse.ArgumentParser()
parser.add_argument("--kit")
parser.add_argument("bioproject")
args = parser.parse_args()

# Remove trailing slash from bioproject if present
args.bioproject = args.bioproject.rstrip('/')

WORK_DIR=os.path.join(os.path.expanduser("~/basecall-work"), args.bioproject)
os.makedirs(WORK_DIR, exist_ok=True)

S3_DIR=os.path.expanduser("~/s3-mnt/nao-restricted/")
def s3_mounted():
    return os.path.exists(os.path.join(S3_DIR, "nao-restricted-exists"))

if not s3_mounted():
    os.makedirs(S3_DIR, exist_ok=True)
    subprocess.check_call(["mount-s3", "--read-only", "nao-restricted", S3_DIR])
assert s3_mounted()

s3_in_dir = os.path.join(S3_DIR, args.bioproject, "pod5")
s3_out_dir = os.path.join(S3_DIR, args.bioproject, "raw")

def batch_input_files(max_files_per_batch=None):
    batches = []
    current_batch = []
    current_batch_size = 0

    for fname in os.listdir(s3_in_dir):
        path = os.path.join(s3_in_dir, fname)
        size = os.path.getsize(path)

        if (current_batch_size + size > BATCH_SIZE or
            len(current_batch) >= max_files_per_batch) and current_batch:

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
batches = list(batch_input_files(max_files_per_batch=1))

existing_output = subprocess.check_output([
    "aws", "s3", "ls",
    "s3://nao-restricted/%s/raw/" % args.bioproject])

for i, batch in enumerate(batches):
    print("Processing batch %s of %s, containing %s files..." % (
        i+1, len(batches), len(batch)))
    for fname in batch:
        print(" ", fname)
    assert batch

    if b"-div%04d.fastq.gz" % i in existing_output:
        print("Already processed div%04d; skipping." % i)
        continue

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

        cmd = [os.path.expanduser("~/dorado-0.8.3-linux-x64/bin/dorado"),
               "basecaller"]
        cmd.extend(["sup", batch_dir])
        if args.kit:
            cmd.extend(["--kit-name", args.kit])

        with open(bam_fname, "w") as outf:
            subprocess.check_call(cmd, stdout=outf)

        shutil.rmtree(batch_dir)

    demux_dir = os.path.join(WORK_DIR, "demux-%i" % i)
    if os.path.exists(demux_dir):
        print("Skipping demultiplexing because already exists.")
    elif not args.kit:
        print("Skipping demultiplexing because no kit was provided.")
    else:
        print("Demultiplexing...")
        subprocess.check_call([
            os.path.expanduser("~/dorado-0.8.3-linux-x64/bin/dorado"),
            "demux",
            "--no-classify",
            "--output-dir", demux_dir,
            bam_fname])

    bam_and_fastqs = []
    if args.kit:
        for demux_bam_leaf in os.listdir(demux_dir):
            assert demux_bam_leaf.endswith(".bam")
            barcode = demux_bam_leaf.replace(
                "%s_barcode" % args.kit, "").replace(".bam", "")
            demux_bam_fname = os.path.join(demux_dir, demux_bam_leaf)
            fastq_gz_div_fname = os.path.join(
                WORK_DIR,
                "%s-%s-div%04d.fastq.gz" % (args.bioproject, barcode, i))
            bam_and_fastqs.append((demux_bam_fname, fastq_gz_div_fname))
    else:
        fastq_gz_div_fname = os.path.join(
            WORK_DIR,
            "%s-div%04d.fastq.gz" % (args.bioproject, i))
        bam_and_fastqs.append((bam_fname, fastq_gz_div_fname))

    for bam_fname, fastq_gz_div_fname in bam_and_fastqs:
        subprocess.check_call([
            "./bam_to_fastq_gz.sh", bam_fname, fastq_gz_div_fname])

        subprocess.check_call([
            "aws", "s3", "cp", fastq_gz_div_fname,
            "s3://nao-restricted/%s/raw/" % args.bioproject])

        os.remove(fastq_gz_div_fname)
        os.remove(bam_fname)

    if args.kit:
        shutil.rmtree(demux_dir)
