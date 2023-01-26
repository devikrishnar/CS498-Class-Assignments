#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext
from operator import add
from collections import defaultdict

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

def splitter(line):
    split_line = line.split(": ")
    split_line2 = split_line[1].split(" ") 
    m = list(map((lambda x : (x, 1)), split_line2))
    m.append((split_line[0], 0))
    return m

def league_check(line, league_list):
    m = []
    if line[0] in league_list:
       m.append(line)
    return m    

lines = sc.textFile(sys.argv[1], 1) 
words = lines.flatMap(splitter)
freqs = words.reduceByKey(add)

leagueIds = sc.textFile(sys.argv[2], 1)
league_list = leagueIds.collect()
league_list_map = freqs.flatMap(lambda j: league_check(j, league_list))
sorted_league_list_map = league_list_map.sortBy(lambda x: -x[1])
sorted_league_list = sorted_league_list_map.collect()

length = len(sorted_league_list)
m1 = []
for i in range(1, length+1):
    rank = length - i
    m1.append((sorted_league_list[i-1][0], rank)) 

rank_list_map = sc.parallelize(m1)
key_sorted_rank_list_map = rank_list_map.sortByKey()

output = open(sys.argv[3], "w")

for (site, rank) in key_sorted_rank_list_map.collect():
    output.write("%s\t%s\n" % (site, rank))

sc.stop()

