#!/usr/bin/env python

from mpi4py import MPI
import json

# MPI setup
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# ndjson_file = "./mastodon-16m.ndjson"

# Test function to have every worker report its rank number, print it, 
# and have the main worker gather all ranks, add them, and report (print) them.
def test_distributing():
    print(f"Hello from worker {rank}! World size is {size}.")

    # Gather all ranks at the main worker (rank 0)
    ranks = comm.gather(rank, root=0)

    # Rank 0 sums and prints the result
    if rank == 0:
        total = sum(ranks)
        print(f"Sum of all ranks: {total}")

if __name__ == "__main__":
    test_distributing()
