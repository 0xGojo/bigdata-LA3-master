import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit
from pyspark.sql.functions import abs
from pyspark.sql import Row
import random

sc = SparkContext()
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("thach")\
        .getOrCreate()

    all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl",
           "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
           "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm",
           "ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd",
           "tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al",
           "bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk",
           "yt", "dengl", "fraspm" ]
    
    random.seed(int(sys.argv[3]))
    centroids = random.sample(all_states, int(sys.argv[2]))

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

    def get_dis_two_states(_state1, _state2, temp_rdd):
        df_check_plantinstate = temp_rdd.withColumn('state1', plant_in_state(plantsRDD_result.items, lit(_state1))).withColumn('state2', plant_in_state(plantsRDD_result.items, lit(_state2)))
        result_df = df_check_plantinstate.withColumn('result', abs(df_check_plantinstate.state1 - df_check_plantinstate.state2))
        return result_df.filter(result_df.result == 1).count()

    all_states = sorted(all_states)

    result = [[]] * int(sys.argv[2])
    for elem in all_states:
        index_centroi = 0
        min_dis = get_dis_two_states(centroids[0], elem, plantsRDD_result)
        index = 0
        for value in centroids[1:]:
            index += 1
            get_new_dis = get_dis_two_states(value, elem, plantsRDD_result) 
            if get_new_dis < min_dis:
                index_centroi = index
                min_dis = get_new_dis
        if len(result[index_centroi]) == 0:
          result[index_centroi] = []
        result[index_centroi].append(elem)

    result = sorted(result)

    

    for index in range(0, int(sys.argv[2])):
      print("* Class " + str(index))
      print(*result[index], end='')
      print()