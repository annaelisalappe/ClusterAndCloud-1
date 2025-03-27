#!/usr/bin/env python

from mpi4py import MPI
import json

# MPI setup
comm = MPI.COMM_WORLD
global_size = comm.Get_size()
rank = comm.Get_rank()

ndjson_file = "./mastodon-16m.ndjson"


def split_and_read_file():
    with open(ndjson_file, 'r', encoding='utf-8') as f:
        f.seek(0, 2)  # Move to the end of the file to get size
        file_size = f.tell()
        chunk_size_per_worker = file_size // global_size  # Divide the file into equal chunks

        start_pos = rank * chunk_size_per_worker
        end_pos = start_pos + chunk_size_per_worker if rank != global_size - 1 else file_size # in case file size not divisible by world size

        first_line = f.readline().strip()
        print(process_line(first_line))

        f.seek(start_pos)
        if rank > 0:
            f.readline()  # Move to the next full line to avoid double-processing

        max_entry = None
        max_sentiment = float("-inf")

        while f.tell() < end_pos:
            line = f.readline()
            if not line:
                break

            entry = process_line(line)
            if entry and entry[1] > max_sentiment:
                max_sentiment = entry[1]
                max_entry = entry

    local_max = (max_sentiment, max_entry if max_entry else (None, None, None, None))
    global_max = comm.allreduce(local_max, op=MPI.MAXLOC)

    # Rank 0 prints the highest sentiment post
    if rank == 0 and global_max[1] != (None, None, None, None):
        print(f"Max sentiment post: {global_max[1]}")

def process_line(line):
    try:
        data = json.loads(line)
        doc = data.get("doc", {})
        created_at = doc.get("createdAt", None)
        sentiment = doc.get("sentiment", None)
        account = doc.get("account", {})
        account_id = account.get("id", None)
        username = account.get("username", None)
        
        if sentiment is not None:
            return (created_at, sentiment, account_id, username)

    except json.JSONDecodeError as e:
        print(f"Error found at line: {line}.")
    return None

if __name__ == "__main__":
    split_and_read_file()


