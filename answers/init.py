import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
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
    
    random.seed(int(sys.argv[2]))
    result = random.sample(all_states, int(sys.argv[1]))
    for elem in result:
    	print(elem)