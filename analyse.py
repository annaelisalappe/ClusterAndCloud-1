#!/usr/bin/env python

from mpi4py import MPI
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# MPI setup
comm = MPI.COMM_WORLD
global_size = comm.Get_size()
rank = comm.Get_rank()


ndjson_file = Path("../mastodon-144g.ndjson").resolve()

def split_and_read_file():

    with open(ndjson_file, 'r', encoding='utf-8') as f:
        if rank == 0:
            print("Successfully opened the file! Reading data...")

        f.seek(0, 2)
        file_size = f.tell()

        # print(f"Actual file size is {file_size}. Pretending that file size is {20000}.")
        # file_size = 10000

        chunk_size_per_worker = file_size // global_size  # Divide the file into equal chunks
        start_pos = rank * chunk_size_per_worker
        end_pos = start_pos + chunk_size_per_worker if rank != global_size - 1 else file_size # in case file size not divisible by world size

        print(f"Rank {rank} reading bytes {start_pos} to {end_pos}.")
        
        f.seek(start_pos)
        if rank > 0:
            f.readline()  # Move to the next full line to avoid double-processing

        user_sentiment = defaultdict(float)
        hour_sentiment = defaultdict(float)

        while f.tell() < end_pos:
            line = f.readline()
            entry = process_line(line)

            if entry:
                hour, sentiment, account_id, username = entry

                if not isinstance(sentiment, (int, float)):
                    print("Warning: 'sentiment' should be of type 'int' or 'float', "
                    f"but got {type(sentiment)}. Value: {sentiment}")
                    continue
                
                if (not isinstance(account_id, str)) or (not isinstance(username, str)):
                    print(f"Warning: 'account_id' or 'username' has an unexpected type. "
                        f"account_id type: {type(account_id)}, value: {account_id}; "
                        f"username type: {type(username)}, value: {username}")
                    continue

                if (sentiment is not None) and account_id and username:
                    user_sentiment[(account_id, username)] += sentiment
                
                if (sentiment is not None) and hour:
                    hour_sentiment[hour] += sentiment
                
                else:
                    print("Parsed the entry but didn't add anything. "
                    f"This is most likely because one of the values was None. The entry was: {entry}")


    # Gather results from all workers
    all_user_sentiment = comm.gather(user_sentiment, root=0)
    all_hour_sentiment = comm.gather(hour_sentiment, root=0) 

    if rank == 0:
        final_user_sentiment = defaultdict(float)
        final_hour_sentiment = defaultdict(float)

        # Combine user sentiment results
        for user_dict in all_user_sentiment:
            for key, value in user_dict.items():
                final_user_sentiment[key] += value

        # Combine hour sentiment results
        for hour_dict in all_hour_sentiment:
            for key, value in hour_dict.items():
                final_hour_sentiment[key] += value

        # Sort users by sentiment score and get the top 5 happiest/saddest users
        sorted_sentiments = sorted(final_user_sentiment.items(), key=lambda x: x[1])
        top_5_unhappiest = sorted_sentiments[:5] # Sorted from low to high, so unhappiest users are at the start
        top_5_happiest = sorted_sentiments[-5:]
        top_5_happiest.reverse()
        
        # Sort hours by sentiment score and get te top 5 happiest/saddest hours
        sorted_hours = sorted(final_hour_sentiment.items(), key=lambda x: x[1])
        top_5_saddest_hours = sorted_hours[:5]
        top_5_happiest_hours = sorted_hours[-5:]
        top_5_happiest_hours.reverse()

        print("Top 5 Happiest Users:")
        for i, ((account_id, username), score) in enumerate(top_5_happiest, 1):
            print(f"({i}) {username}, account id {account_id} with a total sentiment score of {score}")

        print("Top 5 Unhappiest Users:")
        for i, ((account_id, username), score) in enumerate(top_5_unhappiest, 1):
            print(f"({i}) {username}, account id {account_id} with a total sentiment score of {score}")

        print("Top 5 Happiest Hours:")
        for i, (hour, score) in enumerate(top_5_happiest_hours, 1):
            print(f"({i}) {hour} with a total sentiment score of {score}")

        print("Top 5 Saddest Hours:")
        for i, (hour, score) in enumerate(top_5_saddest_hours, 1):
            print(f"({i}) {hour} with a total sentiment score of {score}")

def process_line(line):
    try:
        data = json.loads(line)
        doc = data.get("doc", {})
        created_at = doc.get("createdAt", None)
        sentiment = doc.get("sentiment", None)
        account = doc.get("account", {})
        account_id = account.get("id", None)
        username = account.get("username", None)

        try:
            if created_at: 
                # Extract hour from the timestamp
                dt = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
                hour = dt.strftime("%Y-%m-%d %H:00")  # Format as "YYYY-MM-DD HH:00" (hour level)

                return (hour, sentiment, account_id, username)
            
            print("Warning: No value for 'created_at'. This post is skipped for happiest/ saddest hour.")
            return (None, sentiment, account_id, username)

        except ValueError as e:
            print(f"Got a ValueError while parsing date {dt}. Skipping this line.")
            return (None, sentiment, account_id, username)
        
    except (json.JSONDecodeError) as e:
        print(f"Line at line {line} could not be processed. Skipping.")   # Here we should simply 'pass' instead
        return None

if __name__ == "__main__":
    if rank == 0:
        start_time = MPI.Wtime()

    split_and_read_file()
    if rank == 0:
        end_time = MPI.Wtime()
        total_time = end_time - start_time
        print(f"Execution time: {total_time} seconds.")



