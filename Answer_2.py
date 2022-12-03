from pyspark  import SparkContext 
sc =SparkContext()
def new1(line):
    f= line.split("|")
    return f

rdd1 = sc.textFile("/config/workspace/file_1.txt")
header = rdd1.first()
rdd2 = rdd1.filter(lambda row: row != header)
rdd4= rdd2.map(new1)
rdd6 = rdd4.map(lambda x: ((x[0][-3:] + x[1][:2]),x[7]))
rdd7 = rdd6.map(lambda x : x[0]+"|" + x[1])

rdd7.saveAsTextFile("/config/workspace/final_pric4.txt")
rdd5= rdd7.collect()
for a in rdd5:
    print(a)                                                    