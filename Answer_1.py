
from pyspark  import SparkContext
sc =SparkContext()

def new(line):
    f= line.split("|")
    size=float(f[5])
    price=float(f[6])
    Final_Price= size*price
    return (f[0],f[1],Final_Price)


rdd1 = sc.textFile("/config/workspace/file_1.txt")
header = rdd1.first()
rdd2 = rdd1.filter(lambda row: row != header)
rdd4= rdd2.map(new)
rdd4.saveAsTextFile("/config/workspace/final_pric.txt")
rdd5= rdd4.collect()
for a in rdd5:
    print(a)