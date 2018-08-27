from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("filter").setMaster("local")
sc = SparkContext(conf=conf)

secureLog = sc.textFile("c:\\data\\secure.txt") #secure.txt
wettyFilter = secureLog.filter(lambda wf: wf.split(" ")[4] == "login:") #filter out wetty sessions
print(wettyFilter.count())  #31

failedWettyFilter = wettyFilter.filter(lambda fwf: (str(fwf.split(" ")[5]).lower()) == "failed")    #filter out failed wetty sessions
print(failedWettyFilter.count())    #5

failedWettyFilterMap = failedWettyFilter.map(lambda fwfm: (fwfm.split(" ")[11], fwfm.split(" ")[7]))    #list of failed attempts by key(user)
for i in failedWettyFilterMap.take(5):
    print (i)
