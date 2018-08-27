from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("sucFailedSession").setMaster("local")
sc = SparkContext(conf=conf)

secureLog = sc.textFile("c:\\data\\secureMixed.log")
gw02Filter = secureLog.filter(lambda gw02f: str(gw02f.split(" ")[3]).lower() == "gw02") #filter out only gw02 logs
gw02SshdFilter = gw02Filter.filter(lambda gw02sf: (str(gw02sf.split(" ")[4])[:4]).lower() == "sshd")    #filter out ssh session logs
gw02InavlidFilter = gw02SshdFilter.filter(lambda gw02if: (str(gw02if.split(" ")[5]).lower()) == "invalid")  #filter out invalid session attempts
gw02InvalidUserFilter = gw02InavlidFilter.filter(lambda gw02iuf: str(gw02iuf.split(" ")[6]).lower() == "user")  #filter out invalid attempts by users only

# print(secureLog.count())
# print("gw02 toatl log count = "+str(gw02Filter.count()))
# print("sshdFilter_count = "+str(gw02SshdFilter.count()))
# print("invalid attempt record count = "+str(gw02InavlidFilter.count()))
# print("count of invalid attempts by users only = "+str(gw02InvalidUserFilter.count()))

# #same user on multiple IP
sameUserMultipleIPMap = gw02InvalidUserFilter.\
    map(lambda sumip: (str(sumip.split(" ")[7]).lower(), str(sumip.split(" ")[9])))#, str(sumip.split(" ")[:2])))

# for i in sameUserMultipleIPMap.take(5):
#     print i
#  same ip on multiple users
sameIPMultipleUserMap = gw02InvalidUserFilter.map(lambda sipmu: (sipmu.split(" ")[9], str(sipmu.split(" ")[7]).lower()))    #8961
#sameIPMultipleUserMap = gw02InvalidUserFilter.map(lambda sipmu: (sipmu.split(" ")[9], sipmu))
sameIPMultipleUserSortBykey = sameIPMultipleUserMap.sortByKey(ascending=True)   #8961
# for i in sameIPMultipleUserMap.take(10):
#     print(i)
sameIPMultipeUserUnique = sameIPMultipleUserSortBykey.distinct()  #4185
sameIPMultipeUserGroupByKey = sameIPMultipeUserUnique.groupByKey()  #1211
#sameIPMultipleUserSortedByEachIP = sameIPMultipeUserGroupByKey.flatMap(lambda sipmuseip: (str(sipmuseip[0]), str(list(sipmuseip[1]))))
#sameIPMultipleUserSortedByEachIP = sameIPMultipeUserGroupByKey.\
#    flatMap(lambda sipmuseip: sorted(sipmuseip[1], key=lambda k: str(k.split(" ")[9]).lower()))
sameIPMultipleUserSortedByEachIP = sameIPMultipeUserGroupByKey.\
    flatMap(lambda sipmuseip: (sorted(sipmuseip[1], key=lambda k: str(k[0])), sipmuseip[0]))

# for i in gw02SshdFilter.take(12):
#     print(i)
#
# for i in gw02InavlidFilter.take(12):
#     print i
#gw02InavlidFilter.repartition(1).saveAsTextFile("c:\\data\\invalidRecOutput.txt")
