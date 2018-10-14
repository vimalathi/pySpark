from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("filter").setMaster("local")
sc = SparkContext(conf=conf)

secureLog = sc.textFile("c:\\data\\secure.txt")
# filtering logs of wetty sessions. rest of the logic for analysis will be same as ssh logs
wettyFilter = secureLog.filter(lambda wf: wf.split(" ")[4] == "login:")
print(wettyFilter.count())  #31

