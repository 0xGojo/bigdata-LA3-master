import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit
from pyspark.sql.functions import abs
from pyspark.sql import Row

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
    
    def check_plant_in_state(state_array, state_to_check):
        if state_to_check in state_array:
            return 1
        else :
        	return 0

    plant_in_state = udf(check_plant_in_state)

    state_1 = sys.argv[2] 
    state_2 = sys.argv[3]

    df_check_plantinstate = plantsRDD_result.withColumn('state1', plant_in_state(plantsRDD_result.items, lit(state_1))).withColumn('state2', plant_in_state(plantsRDD_result.items, lit(state_2)))
    result_df = df_check_plantinstate.withColumn('result', abs(df_check_plantinstate.state1 - df_check_plantinstate.state2))
    print(result_df.filter(result_df.result == 1).count())