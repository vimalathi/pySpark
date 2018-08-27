from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("successfulSshSessions")
sc = SparkContext(conf=conf)
#to load data
secureLog = sc.textFile("c:\\data\\secureMixed.log")
#to filter ssh session logs
sshdFilter = secureLog.filter(lambda sf: (str(sf.split(" ")[4])[:4]).lower() == "sshd")     #!ssh - 147367
#to filter out succesfull ssh sessions
acceptedFilter = sshdFilter.filter(lambda af: str(af.split(" ")[5]).lower() == "accepted")  #4903
acceptedFilter.cache()
#distinct users
#distinctUsers = acceptedFilter.map(lambda du: du.split(" ")[8]).distinct().count()  #193
#filtering login using password and publickey sepaprately
passwordFilter = acceptedFilter.filter(lambda pf: str(pf.split(" ")[6]).lower() == "password")  #2560
publickeyFilter = acceptedFilter.filter(lambda pkf: str(pkf.split(" ")[6]).lower() == "publickey")  #2343
#filtering logs of successful ssh seesions until it closes
acceptedFilterMap = acceptedFilter.map(lambda afm: (afm.split(" ")[4], 1))
sshdFilterMap = sshdFilter.map(lambda sfm: (sfm.split(" ")[4], sfm))
acceptedFilterCombinedJoin = acceptedFilterMap.join(sshdFilterMap)  #179167
