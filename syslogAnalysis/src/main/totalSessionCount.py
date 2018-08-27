from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("filter").setMaster("local")
sc = SparkContext(conf=conf)

secureLog = sc.textFile("c:\\data\\secure-20180729")    #24594
sshdFilter = secureLog.filter(lambda sl: (str(sl.split(" ")[4])[:4] == "sshd")) #24589  filter out ssh tools session
sshdMap = secureLog.map(lambda sl: (sl.split(" ")[4], 1))   #get total no of unique sessions whether failled of successed
for i in sshdMap.take(10):
    print i

#noOfSessions = sshdMap.countByKey()
dist = sshdMap.distinct()

sessionCount = dist.count()
print(sessionCount)
