import json
import pymongo
import datetime

def insert_tweet_into_mongo(tweet, collection):
    date = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y') \
           + datetime.timedelta(hours=1)

    htags = []

    for entry in tweet['entities']['hashtags']:
        tag = entry['text']
        htags.append(tag)

    tweet_doc = {
                  '_id': tweet['id'],
                  'user_id': tweet['user']['id'],
                  'user_name': tweet['user']['screen_name'],
                  'text': tweet['text'],
                  'date': datetime.datetime.strftime(date, '%y-%m-%d %H:00'),
                  'tags': htags
                 }

    collection.insert(tweet_doc)

conn=pymongo.MongoClient()
db = conn.hw3
db.drop_collection('tweets')

tweets = db.tweets
hashtags = db.hashtags

files = ['prague-2015-02-14.json', 'prague-2015-02-15.json']

for file in files:
    f = open(file)
    tweets_raw = json.load(f)

    for twt in tweets_raw:
        insert_tweet_into_mongo(twt, tweets)
