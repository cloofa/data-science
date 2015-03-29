import sys, datetime, os
import json
from mrjob.protocol import JSONValueProtocol
from mrjob.job import MRJob

class MRWordCount(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _ , line):
        yield  line['date'], 1

    def reducer(self, key, values):
        yield None, {key: sum(values)}

if __name__ == '__main__':
        MRWordCount.run()
