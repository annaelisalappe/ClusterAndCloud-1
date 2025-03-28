#!/usr/bin/env python

from mpi4py import MPI
import json
from collections import defaultdict

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

        user_sentiment = defaultdict(float)

        while f.tell() < end_pos:
            line = f.readline()
            if not line:
                continue

            entry = process_line(line)
            if entry:
                hour, sentiment, account_id, username = entry
                if account_id and username:
                    user_sentiment[(account_id, username)] += sentiment

    # local_max = (max_sentiment, max_entry if max_entry else (None, None, None, None))
    # global_max = comm.allreduce(local_max, op=MPI.MAXLOC)

    # Rank 0 prints the highest sentiment post
    # if rank == 0 and global_max[1] != (None, None, None, None):
    #     print(f"Max sentiment post: {global_max[1]}")

    # Gather results from all workers
    all_user_sentiment = comm.gather(user_sentiment, root=0)

    if rank == 0:
        final_user_sentiment = defaultdict(float)
        for user_dict in all_user_sentiment:
            for key, value in user_dict.items():
                final_user_sentiment[key] += value

        # Sort users by sentiment score and get the top 5 happiest users
        sorted_sentiments = sorted(final_user_sentiment.items(), key=lambda x: x[1], reverse=True)
        top_5_happiest = sorted_sentiments[:5]
        top_5_unhappiest = sorted_sentiments[-5:].reverse()
        
        print("Top 5 Happiest Users:")
        for i, ((account_id, username), score) in enumerate(top_5_happiest, 1):
            print(f"({i}) {username}, account id {account_id} with a total sentiment score of {score:.2f}")

        print("Top 5 Unhappiest Users:")
        for i, ((account_id, username), score) in enumerate(top_5_unhappiest, 1):
            print(f"({i}) {username}, account id {account_id} with a total sentiment score of {score:.2f}")


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


