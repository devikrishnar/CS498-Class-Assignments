#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext
from operator import add
from collections import defaultdict

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

def splitter(line):
    split_line = line.split(": ")
    split_line2 = split_line[1].split(" ")
    
    m = list(map((lambda x : (x, 1)), split_line2))
    m.append((split_line[0], 0))
    return m

lines = sc.textFile(sys.argv[1], 1) 
words = lines.flatMap(splitter)
freqs = words.reduceByKey(add)
top10sites = freqs.takeOrdered(10, key = lambda x: -x[1])

output = open(sys.argv[2], "w")

d = defaultdict(list)
sorted_d = defaultdict(list)

for (site, count) in top10sites:
    d[site].append(count)

for key in sorted(d.keys()):
    sorted_d[key].append(d[key][0])

for key in sorted_d:
    output.write("%s\t%s\n" % (key, sorted_d[key][0]))

sc.stop()

