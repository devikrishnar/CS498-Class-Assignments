#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext
from operator import add

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
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
orphan_pages = freqs.filter(lambda x : int(x[1]) == 0)
sorted_orphan_pages = orphan_pages.sortByKey()

output = open(sys.argv[2], "w")

for (page, freq) in sorted_orphan_pages.collect():
    output.write("%s\n" % (page))

sc.stop()

