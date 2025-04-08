#!/bin/bash
#SBATCH --time=01:00:00
#SBATCH --job-name=anna(e)lisas_job
#SBATCH --output=small-1n-8c/output.out
#SBATCH --error=small-1n-8c/error.err

ml GCCcore/11.3.0 Python/3.11.3 OpenMPI/4.1.4 mpi4py/3.1.4

run_and_measure() {
    start=$(date +%s.%N)
    srun python3 -u analyse.py
    end=$(date +%s.%N)
    runtime=$(echo "$end - $start" | bc)
    echo "$runtime"
}

if [[ "$1" == "test_only" ]]; then
    srun --output=outputs/distributed_output.out python3 analyse.py &
    srun --ntasks=1 --nodes=1 --exclusive --output=outputs/non_distributed_output.out python3 run_non_distributed.py &
    wait 
    diff outputs/distributed_output.out outputs/non_distributed_output.out

# NOTE: Only for running on the smaller files!
elif [[ "$1" == "benchmark_only" ]]; then
    runtimes=()
    for i in {1..3}; do
        echo "Benchmark run $i..."
        runtime=$(run_and_measure)
        runtimes+=("$runtime")
        echo "Runtime $i: $runtime seconds"
    done

    avg=$(printf "%s\n" "${runtimes[@]}" | awk '{sum+=$1} END {print sum/NR}')
    echo "Average runtime over 3 runs: $avg seconds"

else
    srun python3 -u analyse.py
fi