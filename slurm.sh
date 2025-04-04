#!/bin/bash
#SBATCH --time=01:00:00
#SBATCH --job-name=anna(e)lisas_job
#SBATCH --output=outputs-1n-1c/output.out
#SBATCH --error=outputs-1n-1c/error.err

ml GCCcore/11.3.0 Python/3.11.3 OpenMPI/4.1.4 mpi4py/3.1.4

if [[ "$1" == "test_only" ]]; then
    srun --output=outputs/distributed_output.out python3 analyse.py &
    srun --ntasks=1 --nodes=1 --exclusive --output=outputs/non_distributed_output.out python3 run_non_distributed.py &
    wait 
    diff outputs/distributed_output.out outputs/non_distributed_output.out
else
    srun python3 -u analyse.py
fi