import sys
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

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

    getFrequentItems = plants_withID.select("id", "items")
    fpGrowth = FPGrowth(itemsCol="items", minSupport=float(sys.argv[3]), minConfidence=float(sys.argv[4]))
    model = fpGrowth.fit(getFrequentItems)

    def get_antecedent_length(antecedent):
        return len(antecedent)

    antecedent_length_func = udf(get_antecedent_length, IntegerType())
    
    freq_item_table = model.associationRules.select("antecedent","consequent","confidence", antecedent_length_func("antecedent").alias("antecedent_length"))
    #.orderBy("items", "freq").show(int(sys.argv[2]))
    freq_item_table.createOrReplaceTempView("fre_antecedent_result")


    result_rows = sys.argv[2]
    spark.sql("SELECT antecedent, consequent, confidence FROM fre_antecedent_result ORDER BY antecedent_length desc, confidence desc").show(int(result_rows))