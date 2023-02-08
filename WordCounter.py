import re

from mrjob.job import MRJob

WORD_REGEXP = re.compile(r"[\w']+")


class MRWordCounter(MRJob):

    def mapper(self, _, line):
        # words = line.split()
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordCounter.run()