#!/bin/bash
#SBATCH --time=0:05:0
#SBATCH --nodes=2
#SBATCH --job-name=anna(e)lisas_test
#SBATCH --ntasks=4 
#SBATCH --ntasks-per-node=4
#SBATCH --output=outputs/output.out
#SBATCH --error=outputs/error.err

ml GCCcore/11.3.0 Python/3.11.3 OpenMPI/4.1.4 mpi4py/3.1.4

if [[ "$1" == "test_only" ]]; then
    srun --output=outputs/distributed_output.out python3 analyse.py &
    srun --ntasks=1 --nodes=1 --exclusive --output=outputs/non_distributed_output.out python3 run_non_distributed.py &
    wait 
    diff outputs/distributed_output.out outputs/non_distributed_output.out
else
    srun python3 analyse.py
fi