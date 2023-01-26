from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType

sc = SparkContext()
sqlContext = SQLContext(sc)

####
# 1. Setup (10 points): Download the gbook file and write a function to load it in an RDD & DataFrame
####

def splitter(line):
    split_line = line.split('\t')
    return split_line

lines = sc.textFile("gbooks")
rdd = lines.map(splitter)
#schema_lst = ["place","count1","count2","count3"]
#converted_df = lines.toDF(schema_lst)
#converted_df.printSchema()
#converted_df.show()



schema = StructType([StructField("word", StringType(), True), StructField("count1", IntegerType(), True), StructField("count2", IntegerType(), True), StructField("count3", IntegerType(), True)])
df3 = sqlContext.createDataFrame(rdd, schema)
df3.printSchema()


#val dfFromRDD = lines.toDF("place","count1", "count2", "count3")
#dfFromRDD.printSchema()
# RDD API
# Columns:
# 0: place (string), 1: count1 (int), 2: count2 (int), 3: count3 (int)


# Spark SQL - DataFrame API



