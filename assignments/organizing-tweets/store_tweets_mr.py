import json
import pymongo
import datetime


fname = "tweets-organized.json"
out = open(fname,"w")

def write(tweet_doc):
    out.write(json.dumps(tweet_doc).encode('utf8'))
    out.write('\n')
def store_formatted_tweet(tweet):
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

    write(tweet_doc)

files = ['prague-2015-02-14.json', 'prague-2015-02-15.json']

for file in files:
    f = open(file)
    tweets_raw = json.load(f)

    for twt in tweets_raw:
        store_formatted_tweet(twt)

out.close()
