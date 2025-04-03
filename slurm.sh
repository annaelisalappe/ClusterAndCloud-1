#!/bin/bash
#SBATCH --time=0:05:0
#SBATCH --nodes=2
#SBATCH --job-name=anna(e)lisas_test
#SBATCH --ntasks=4 
#SBATCH --ntasks-per-node=4

ml GCCcore/11.3.0 Python/3.11.3 OpenMPI/4.1.4 mpi4py/3.1.4

if [[ "$1" == "test_only" ]]; then
    srun python3 ClusterAndCloud-1/analyse.py > outputs/distributed_output.txt &
    srun --ntasks=1 --nodes=1 --exclusive python3 outputs/ClusterAndCloud-1/run_non_distributed.py > non_distributed_output.txt &
    wait 
    diff distributed_output.txt non_distributed_output.txt
else
    srun python3 ClusterAndCloud-1/analyse.py
fi