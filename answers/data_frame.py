import sys
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import monotonically_increasing_id

sc = SparkContext()
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("thach")\
        .getOrCreate()

    lines = sc.textFile(sys.argv[1])
    parts = lines.map(lambda l: l.split(",", 1))
    parts = parts.map(lambda l: [l[0], l[1].split(",")])
    plantsRDD = parts.map(lambda p: Row(plant=p[0], items=p[1]))
                             
    plantsRDD_result = spark.createDataFrame(plantsRDD)

    plants_withID = plantsRDD_result.orderBy('plant').withColumn("id", monotonically_increasing_id())
    plants_withID.createOrReplaceTempView("plant_states")
    spark.sql("SELECT id, plant, items FROM plant_states").show(int(sys.argv[2]))