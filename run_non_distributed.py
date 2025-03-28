import json
import heapq
from collections import defaultdict

def analyze_sentiment(file):
    user_sentiments = defaultdict(float)

    with open(file, "r", encoding="utf-8") as f:
        for line in f:
            created_at, sentiment, account_id, username = process_line(line)

            if account_id and username and sentiment is not None:
                user_sentiments[(username, account_id)] += sentiment

    sorted_sentiments = sorted(user_sentiments.items(), key=lambda x: x[1])
    top_5_unhappiest = sorted_sentiments[:5] # Sorted from low to high, so unhappiest users are at the start
    top_5_happiest = sorted_sentiments[-5:]
    top_5_happiest.reverse()
    
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

analyze_sentiment("./mastodon-16m.ndjson")