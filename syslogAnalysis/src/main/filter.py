from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("filter").setMaster("local")
sc = SparkContext(conf=conf)

secureLog = sc.textFile("c:\\data\\secure-20180729")

#secureLogTake = secureLog.take(12)  #its a unicode text file

#for i in secureLog.take(12):
 #   print i
    #print i[1:3]
#def sshdFilter():

#julMonthFilter = secureLog.filter(lambda sm: (sm.split(" ")[0] == "Jul"))  #count=315308
#junMonthFilter = secureLog.filter(lambda sm: (sm.split(" ")[0] == "Jun"))

sshdMap = secureLog.map(lambda sl: (sl.split(" ")[4], 1))
for i in sshdMap.take(10):
    print i

noOfSessions = sshdMap.countByKey()

sshdFilter = secureLog.filter(lambda sl: (str(sl.split(" ")[4])[:4] == "sshd"))


