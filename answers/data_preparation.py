import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
import codecs

sc = SparkContext()
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("thach")\
        .getOrCreate()

    lines = sc.textFile(sys.argv[1])
    parts = lines.map(lambda l: l.split(",", 1))
    parts = parts.map(lambda l: [(l[0]), (l[1].split(","))])
    get_states_of_plant = parts.groupByKey().mapValues(list).lookup((sys.argv[2]))
    
    for elem in get_states_of_plant:
        result_state = elem[0]
    state = sys.argv[3]
    file = codecs.open(sys.argv[4], "w")
    result = 0
    # print(result_state)
    if state in result_state:
        result = 1 
    # print(result)
    file.write(str(result))
