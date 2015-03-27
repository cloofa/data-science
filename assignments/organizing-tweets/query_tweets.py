import json
import pymongo
import datetime
from bson.son import SON

conn=pymongo.MongoClient()
db = conn.hw3
tweets = db.tweets
hashtags = db.hashtags

hours = range(9, 17)
days  = range(14,16)
query_dates = []

for d in days:
    for h in hours:
        date = datetime.datetime(2015, 2, d, h)
        query_dates.append(datetime.datetime.strftime(date, '%y-%m-%d %H:00'))


pipeline = [

            {"$group": { "_id": "$date", "count": {"$sum": 1}} },

            {"$match": {"_id": {"$in": query_dates }}}

           ]

query1 = db.tweets.aggregate(pipeline)
print query1['result']

pipeline = [

            {"$project": { "user_id": 1, "user_name": 1, "date": 1} },

            {"$match": {"date": {"$in": query_dates } } },

            {"$group": { "_id": {"id": "$user_id", "name": "$user_name"},
                         "count": {"$sum": 1} } },

            {"$sort": { "count": -1}},

            {"$limit": 1}

           ]

query2 = db.tweets.aggregate(pipeline)
print query2['result']

pipeline = [

            {"$project": { "tags": 1, "date": 1} },

            {"$match": {"date": {"$in": query_dates } } },

            {"$unwind": "$tags"},

            {"$group": { "_id": "$tags", "count": {"$sum": 1} } },

            {"$sort": { "count": -1}},

            {"$limit": 10}

            ]


query3 = db.tweets.aggregate(pipeline)
print query3['result']



#for date in query_dates:
#    pipeline = [
#                    {"$project": { "tags": 1, "date": 1} },
#                    {"$match": {"date": date}},
#                    {"$unwind": "$tags"},
#                    {"$group": { "_id": "$tags", "count": {"$sum": 1}} },
#                ]
#    querytmp = db.tweets.aggregate(pipeline)
#    query2.append(querytmp['result'])
