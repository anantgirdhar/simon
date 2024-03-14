#!/bin/bash
#SBATCH -J sample_job_name
#SBATCH -A sample_queue_name
#SBATCH -N 1
#SBATCH --ntasks=2
#SBATCH --mem-per-cpu=2G
#SBATCH -t 00:10:00
#SBATCH -q embers
#SBATCH -o Report-%j.out

echo "Starting job..."
sleep 30
echo "Job complete!"
