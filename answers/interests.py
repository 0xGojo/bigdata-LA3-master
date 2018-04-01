import sys
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import udf, lit
from pyspark.sql.functions import abs

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
    # plants_withID.createOrReplaceTempView("plant_states")	

    getFrequentItems = plants_withID.select("id", "items")
    fpGrowth = FPGrowth(itemsCol="items", minSupport=float(sys.argv[3]), minConfidence=float(sys.argv[4]))
    model = fpGrowth.fit(getFrequentItems)

    def get_antecedent_length(antecedent):
        return len(antecedent)

    antecedent_length_func = udf(get_antecedent_length)

    total_func = plants_withID.count()
    frequent = model.freqItemsets
    association = model.associationRules

    table_df = association.join(frequent, frequent.items == association.consequent)
    table_df = table_df.withColumn("interest",abs(table_df.confidence - table_df.freq / lit(total_func))).withColumn("antecedent_length" ,antecedent_length_func(table_df.antecedent))

    table_df.createOrReplaceTempView("freq_consequent_result")
    spark.sql("SELECT antecedent, consequent, confidence, items, freq, interest FROM freq_consequent_result ORDER BY antecedent_length desc, interest desc").show(int(sys.argv[2]))