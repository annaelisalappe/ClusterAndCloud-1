#!/bin/bash
#SBATCH --time=0:05:0
#SBATCH --nodes=1
#SBATCH --job-name=anna(e)lisas_test
#SBATCH --ntasks=4

ml GCCcore/11.3.0 Python/3.11.3 OpenMPI/4.1.4 mpi4py/3.1.4

srun -n 4 python3 ClusterAndCloud-1/analyse.py