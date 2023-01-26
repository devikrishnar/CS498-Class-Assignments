from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType

sc = SparkContext()
sqlContext = SQLContext(sc)

####
# 1. Setup (10 points): Download the gbook file and write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: place (string), 1: count1 (int), 2: count2 (int), 3: count3 (int)


# Spark SQL - DataFrame API

def splitter(line):
    split_line = line.split('\t')
    for i in range(1, len(split_line)):
        split_line[i] = int(split_line[i])
    return split_line

lines = sc.textFile("gbooks")
rdd = lines.map(splitter)
schema = StructType([StructField("word", StringType(), True), StructField("count1", IntegerType(), True), StructField("count2", IntegerType(), True), StructField("count3", IntegerType(), True)])
df = sqlContext.createDataFrame(rdd, schema)

df.createOrReplaceTempView('gbookstb')
sqlContext.sql("select word, COUNT(1) from gbookstb group by word order by count(word) DESC").show(3)

####
# 4. MapReduce (10 points): List the three most frequent 'word' with their count of appearances
####

# Spark SQL

# There are 18 items with count = 425, so could be different 
# +---------+--------+
# |     word|count(1)|
# +---------+--------+
# |  all_DET|     425|
# | are_VERB|     425|
# |about_ADP|     425|
# +---------+--------+

