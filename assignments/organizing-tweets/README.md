# Organizing Acquired Data #
For all three storage technologies:

 * I change the datetime format to ‘%y-%m-%d %H:00:00’
 * I convert the tweet creation time to local time by adding one hour

# NoSQL #
store_tweets.py: script that parses the tweet input and inserts json documents into MongoDB with the following schema

	'_id': tweet id,
	'user_id': user id,
	'user_name': user screen name,
	'text': text,
	'date': tweet creation time,
	'tags': all hashtags in text
	     
query_tweets.py: script that queries the data from MongoDB using pymongo.  the procedures are a combination of MongoDB  aggregation methods.

Results are in query_results.txt

# Key/Value store #
I did not implement this fully, but I thought code for querying would be easier than pseudocode. Data would be stored in documents per hour.  The input to each map reduce job is all documents between hours 0900 and 1600

mr_query_hours.py: mapper collects tweets for every hour, reducer counts tweets per hour (though I can simply count number of tweets in each file, as a much simpler way to answer this question)  

mr_query_users.py: mapper collects tweets by user, reducer counts tweets by user, final_reducer finds the max user by count.

mr_query_tags.py: mapper collects tags in tweets, reducer sums count per tag, final_reducer sort and outputs top 10 tags.
