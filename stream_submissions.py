import json

import praw
import boto3
from profanity_check import predict, predict_prob

import vars


# Instantiate a reddit object
reddit = praw.Reddit(
    client_id=vars.client_id,
    client_secret=vars.client_secret,
    user_agent=vars.user_agent,
)

# Instantiate a kinesis client
kinesis = boto3.client("kinesis")

list_target = []
count = 0
submissions = reddit.subreddit("all").stream.submissions()


try:
    while True:
        try:
            for s in submissions:
                # Check if title is profane
                profanity_array = []
                profanity_array.append(s.title)
                profanity_flag = predict(profanity_array)
                # Dump to json
                jsonItem = json.dumps(
                    {
                        "title": s.title,
                        "sub": s.subreddit_name_prefixed,
                        "author": s.author.fullname,
                        "nsfw": s.over_18,
                        "profanity_flag": int(profanity_flag[0])
                    }
                )

                list_target.append({"Data": jsonItem, "PartitionKey": "filler"})
                count += 1
                print(count)
                if count == 50:
                    kinesis.put_records(StreamName="reddit", Records=list_target)
                    print("----------")
                    print("SENDING RECORDS TO KINESIS")
                    print("----------")
                    count = 0
                    list_target = []

        except Exception as e:
            print()
            print(f"Exception: {e}")
            print()

except KeyboardInterrupt:
    pass


# with open('data.json', 'w') as outfile:
#     json.dump(list_target, outfile)

print()
print("ended")
