import sys
import json
import os
import datetime
from mrjob.job import MRJob


hours = range(9, 17)
days  = range(14,16)
query_dates = []

for d in days:
    for h in hours:
        date = datetime.datetime(2015, 2, d, h)
        query_dates.append(datetime.datetime.strftime(date, '%y-%m-%d %H:00'))


class MRWordCount(MRJob):

    def mapper(self, _ , line):
        obj = json.loads(line)
	if obj['date'] in query_dates:
            obj['date'], 1 

    def reducer(self, key, values):
        yield key, sum(occurrences)

if __name__ == '__main__':
        MRWordCount.run()


