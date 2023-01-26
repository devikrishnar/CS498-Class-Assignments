#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
import re
from pyspark import SparkConf, SparkContext
from operator import add
from collections import defaultdict

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]


with open(stopWordsPath) as f:
	#TODO
    data = f.read()
    stopWords = data.split("\n")

with open(delimitersPath) as f:
    #TODO
    delimiters = str(f.read())

def splitter(line):
    regexPattern = '|'.join(map(re.escape, delimiters))
    split_line = re.split(regexPattern, line)

    resultwords  = [word for word in split_line if word.lower() not in stopWords]

    return map(str.lower, resultwords)

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

#TODO
words = lines.flatMap(splitter)
new_words = words.filter(lambda x: x!="")
pairs = new_words.map(lambda word: (word, 1))
freqs = pairs.reduceByKey(add)
top10words = freqs.takeOrdered(10, key = lambda x: -x[1])

outputFile = open(sys.argv[4],"w")

d = defaultdict(list)
sorted_d = defaultdict(list)
#TODO
for (word, count) in top10words:
    d[word].append(count)

for key in sorted(d.keys()):
    sorted_d[key].append(d[key][0])

for key in sorted_d:
    outputFile.write("%s\t%s\n" % (key, sorted_d[key][0]))
#write results to output file. Foramt for each line: (line +"\n")

sc.stop()

