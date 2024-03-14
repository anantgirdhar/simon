#!/bin/bash
#SBATCH -J sample_compress_job_name
#SBATCH -A sample_compress_queue_name
#SBATCH -N 1
#SBATCH --ntasks=2
#SBATCH --mem-per-cpu=2G
#SBATCH -t 08:00:00
#SBATCH -q embers
#SBATCH -o Report-%j_compress.out

# Go to the appropriate run directory
cd $SLURM_SUBMIT_DIR
