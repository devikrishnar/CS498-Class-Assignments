#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)
words = lines.flatMap(lambda line: line.split())

count = []
words_list = words.collect()

for i in range(1, len(words_list), 2):
    count.append(int(words_list[i]))

count.sort()

ans3 = count[0]
ans4 = count[len(count)-1]
ans2 = sum(count)
ans1 = int(ans2/len(count))
sqDiff = 0

for i in count:
    sqDiff += (i - ans1) * (i - ans1)

ans5 = int(sqDiff/len(count))

outputFile = open(sys.argv[2], "w")

#TODO write your output here
#write results to output file. Format
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)

sc.stop()

