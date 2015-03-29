import sys, datetime, os, operator
import json
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep
from mrjob.job import MRJob

output_tags = []

class MRWordCount(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _ , line):
        for tag in line['tags']:
            yield tag, 1

    def reducer(self, key, values):
        output_tags.append((sum(values), key))

    def reducer_sort_tags(self):
        map(operator.itemgetter(0), output_tags)
        sorted_tags = sorted(output_tags, key=operator.itemgetter(0), reverse=True)
        for tag in sorted_tags[:10]:
            yield tag

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer,
                   reducer_final=self.reducer_sort_tags)
                ]

if __name__ == '__main__':
        MRWordCount.run()
