from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName("transformation").setMaster("local")
sc = SparkContext(conf=conf)

secureLog = sc.textFile("c:\\data\\secureMixed.log")    #count - 1166245
#to filter ssh session logs
sshdFilter = secureLog.filter(lambda sf: (str(sf.split(" ")[4])[:4]).lower() == "sshd")     #!ssh - 147367
acceptedFilter = sshdFilter.filter(lambda af: str(af.split(" ")[5]).lower() == "accepted")  #4903
# acceptedFilterMap = acceptedFilter.\
#     map(lambda afm: (str(afm.split(" ")[0]) + str(afm.split(" ")[1]) + str(afm.split(" ")[4]), afm))
# sshdFilterMap = sshdFilter.\
#     map(lambda sfm: (str(sfm.split(" ")[0]) + str(sfm.split(" ")[1]) + str(sfm.split(" ")[4]), sfm))
# acceptedCombined = sshdFilterMap.join(acceptedFilterMap)    #count 23759
# acceptedCombined2 = acceptedFilterMap.join(sshdFilterMap).isin()
acceptedFilterTolist = acceptedFilter.map(lambda aft: aft.split(" ")[4]).toLocalIterator()
generatorTolist for i in list(acceptedFilterTolist)
generatorTolist = str(list(acceptedFilterTolist))
acceptedFilterIsin = sshdFilter.filter(lambda afi: str(afi.split(" ")[4]) in (list(acceptedFilterTolist)))
