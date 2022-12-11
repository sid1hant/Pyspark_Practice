from pyspark  import SparkContext
sc =SparkContext()

def new(line):
    f= line.split("|")
    w=int(f[1])
    return f[0],w

rdd1 = sc.textFile("/config/workspace/file_6.txt")
header = rdd1.first()
rdd2 = rdd1.filter(lambda row: row != header)
rdd4= rdd2.map(new)
rdd5=rdd4.sortBy(lambda x : x[1],False)
rdd6=rdd5.map(lambda x :x[0])
rdd7=rdd6.collect()
rdd8=rdd7[1]
print(rdd8)
