# basecall

Wrapper around Dorado for basecalling Nanopore files

## Choosing the right EC2 instance
To run the Dorado basecaller, you need a machine with a GPU that supports CUDA drivers. According to [public benchmarking data](https://aws.amazon.com/blogs/hpc/benchmarking-the-oxford-nanopore-technologies-basecallers-on-aws/), g5.xlarge is the most cost-effective machine for basecalling. We use this EC2 instance with the Deep Learning Base OSS Nvidia Driver AMI (Amazon Linux 2) (AMI identifier: ami-069699babcf73a576).

## Install dependencies
 - dorado 0.7.3: (https://github.com/nanoporetech/dorado)
 - samtools: (https://www.htslib.org/download/)
 - mount-s3: (https://github.com/awslabs/mountpoint-s3/tree/main)

