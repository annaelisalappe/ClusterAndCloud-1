import json
import heapq
from collections import defaultdict
from datetime import datetime
import timeit


def analyze_sentiment(file):
    user_sentiments = defaultdict(float)
    hour_sentiments = defaultdict(float)

    with open(file, "r", encoding="utf-8") as f:
        for line in f:
            entry = process_line(line)

            if entry:
                hour, sentiment, account_id, username = entry
                if sentiment and account_id and username:
                    user_sentiments[(account_id, username)] += sentiment
                
                if sentiment and hour:
                    hour_sentiments[hour] += sentiment

    sorted_sentiments = sorted(user_sentiments.items(), key=lambda x: x[1])
    top_5_unhappiest = sorted_sentiments[:5] # Sorted from low to high, so unhappiest users are at the start
    top_5_happiest = sorted_sentiments[-5:]
    top_5_happiest.reverse()

    # Sort hours by sentiment score and get te top 5 happiest/saddest hours
    sorted_hours = sorted(hour_sentiments.items(), key=lambda x: x[1])
    top_5_saddest_hours = sorted_hours[:5]
    top_5_happiest_hours = sorted_hours[-5:]
    top_5_happiest_hours.reverse()
    
    print("Top 5 Happiest Users:")
    for i, ((account_id, username), score) in enumerate(top_5_happiest, 1):
        print(f"({i}) {username}, account id {account_id} with a total sentiment score of {score:.2f}")

    print("Top 5 Unhappiest Users:")
    for i, ((account_id, username), score) in enumerate(top_5_unhappiest, 1):
        print(f"({i}) {username}, account id {account_id} with a total sentiment score of {score:.2f}")

    print("Top 5 Happiest Hours:")
    for i, (hour, score) in enumerate(top_5_happiest_hours, 1):
        print(f"({i}) {hour} with a total sentiment score of {score:.2f}")

    print("Top 5 Saddest Hours:")
    for i, (hour, score) in enumerate(top_5_saddest_hours, 1):
        print(f"({i}) {hour} with a total sentiment score of {score:.2f}")

def process_line(line):
    try:
        data = json.loads(line)
        doc = data.get("doc", {})
        created_at = doc.get("createdAt", None)
        sentiment = doc.get("sentiment", None)
        account = doc.get("account", {})
        account_id = account.get("id", None)
        username = account.get("username", None)
        
        if created_at: 
            # Extract hour from the timestamp
            dt = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
            hour = dt.strftime("%Y-%m-%d %H:00")  # Format as "YYYY-MM-DD HH:00" (hour level)
        
        return (hour, sentiment, account_id, username)

    except json.JSONDecodeError as e:
        print(f"Line at line {line} could not be processed. Skipping.")   # Here we should simply 'pass' instead
    return None

start_time = timeit.timeit()
analyze_sentiment("./mastodon-16m.ndjson")
end_time = timeit.timeit()
total_time = end_time - start_time
print(f"Execution time: {total_time} seconds.")