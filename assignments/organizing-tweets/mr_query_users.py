import sys, datetime, os
import json
from mrjob.protocol import JSONValueProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordCount(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _ , line):
	    yield line['user_name'], 1

    def reducer(self, key, values):
        yield None, (sum(values), key)

    def reducer_find_max_user(self, _, user_count_pairs):
        yield max(user_count_pairs)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_find_max_user)
        ]
if __name__ == '__main__':
        MRWordCount.run()
