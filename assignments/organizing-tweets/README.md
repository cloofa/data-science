# Organizing Acquired Data #
For all three storage technologies:

 * I change the datetime format to ‘%y-%m-%d %H:00:00’
 * I convert the tweet creation time to local time by adding one hour

store_tweets.py: script that parses the tweet input and inserts json documents into MongoDB with the following schema

	'_id': tweet id,
	'user_id': user id,
	'user_name': user screen name,
	'text': text,
	'date': tweet creation time,
	'tags': all hashtags in text
	     
query_tweets.py: script that queries the data from MongoDB using pymongo.  the procedures are a combination of MongoDB  aggregation methods.

Results are in query_results.txt
