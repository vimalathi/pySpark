from pyspark import SparkContext, SparkConf
from datetime import datetime
import sys;

# if len(sys.argv) != 3:
#     print ("<runLocal> <inputPath> <outputPath>")
#     sys.exit(0)
#
# if str(sys.argv[0]).lower() == "runlocal":
#     conf = SparkConf().setAppName("transformation").setMaster("local[2]")
#     conf.set("spark.broadcast.compress", "false")
#     conf.set("spark.shuffle.compress", "false")
#     conf.set("spark.shuffle.spill.compress", "false")
#     sc = SparkContext(conf=conf)
# else:
#     conf = SparkConf().setAppName("logAnalysis")
#     sc = SparkContext(conf=conf)
conf = SparkConf().setAppName("transformation").setMaster("local[2]")
sc = SparkContext(conf=conf)


def timeDifference(x, y):
    timeformat = "%H:%M:%S"
    # if x == "":
    #     x = str(datetime.now().strftime(timeformat))
    # if y == "":
    #     y = str(datetime.now().strftime(timeformat))
    a = datetime.strptime(x, timeformat)
    b = datetime.strptime(y, timeformat)
    if a > b:
        secs = (a - b).total_seconds()
        return (secs / 61) / 60
    else:
        secs = (b - a).total_seconds()
        return (secs / 61) / 60


secureLog = sc.textFile("c:\\data\\secureMixed2.log").persist()  # count - 1166245
# to filter ssh session logs
sshdFilter = secureLog.filter(lambda sf: (str(sf.split(" ")[4])[:4]).lower() == "sshd")
openedClosedFilter = sshdFilter.filter(lambda ocf: str(ocf.split(" ")[7]).lower() in ["opened", "closed"])  # 9735
openedClosedFilterDistinct = openedClosedFilter.distinct()  # 9602
openedClosedMap = openedClosedFilterDistinct.map(lambda csm: (
    str(csm.split(" ")[0]) + " " + str(csm.split(" ")[1]) + " " + str(csm.split(" ")[3]) + " " + str(
        csm.split(" ")[4]) + " " + csm.split(" ")[10], csm))  # total 9602 #dist kval 9602 #dist keys 5014
openedSession = openedClosedMap.filter(
    lambda os: str((os[1]).split(" ")[7]).lower() == "opened")  # count 4829 #dist keys 4829
closedSession = openedClosedMap.filter(
    lambda cs: str((cs[1]).split(" ")[7]).lower() == "closed")  # total, dist keys 4773 #diff 56
closedSessionKeys = closedSession.keys().collect()
openedSessionKeys = openedSession.keys().collect()
openedAndClosedSession = openedSession.filter(lambda oacs: oacs[0] in closedSessionKeys)  # 4588
openedAndClosedSession2 = closedSession.filter(lambda oacs: oacs[0] in openedSessionKeys)  # 4588
openedAndClosedSessionUnion = openedAndClosedSession.union(openedAndClosedSession2)  # 9176
openedClosedSessionKeyValue = openedAndClosedSessionUnion.map(lambda ocskv: (ocskv[0], str(ocskv[1]).split(" ")[2]))
timeFormat = "%H:%M:%S"
sessionUsageInHoursBykey = openedClosedSessionKeyValue.reduceByKey(lambda stu1, stu2: timeDifference(stu1, stu2))
sessionUsageInHoursBykey.collect()  # 5014
# openedSessionFilter = openedClosedFilterDistinct.filter(lambda osf: str(osf.split(" ")[7]).lower() == "opened")  # 4830
# closedSessionFilter = openedClosedFilterDistinct.filter(
#     lambda csf: str(csf.split(" ")[7]).lower() == "closed")  # 4774 #56 diff
# openedSessionMap = openedSessionFilter.map(
#     lambda osm: (str(osm.split(" ")[0]) + " " + str(osm.split(" ")[1]) + " " + str(osm.split(" ")[3]) + " " + str(
#         osm.split(" ")[4]) + " " + osm.split(" ")[10], osm))  # total 4830 #dist key 4829
# closedSessionMap = closedSessionFilter.map(
#     lambda csm: (str(csm.split(" ")[0]) + " " + str(csm.split(" ")[1]) + " " + str(csm.split(" ")[3]) + " " + str(
#         csm.split(" ")[4]) + " " + csm.split(" ")[10], csm))  # total 4774 #dist key 4773
# openedSessionKeys = openedSessionMap.keys().collect()
# set(x for x in openedSessionKeys if openedSessionKeys.count(x) > 1) #set([u'Jul 21 gw02 sshd[26692]: itversitylab'])
# closedSessionKeys = closedSessionMap.keys().collect()
# set(x for x in closedSessionKeys if closedSessionKeys.count(x) > 1) #set([u'Jul 21 gw02 sshd[26692]: itversitylab'])

# ----------------------------------------------------------------------------------------------------------
# acceptedFilter = sshdFilter.filter(lambda af: str(af.split(" ")[5]).lower() == "accepted")  # 4903
# acceptedKeys = acceptedFilter.map(
#     lambda sm: (
#         str(sm.split(" ")[0]) + " " + str(sm.split(" ")[1]) + " " + str(sm.split(" ")[3]) + " " + str(sm.split(" ")[4]),
#         sm)).keys()
# keysList = acceptedKeys.collect()
#
# sshdMap = sshdFilter.map(
#     lambda sm: (
#         str(sm.split(" ")[0]) + " " + str(sm.split(" ")[1]) + " " + str(sm.split(" ")[3]) + " " + str(sm.split(" ")[4]),
#         sm))
# sshdMatchingKeyValues = sshdMap.filter(lambda smk: smk[0] in keysList)  # 17790
# openedAndClosedFilterKeyValues = sshdMatchingKeyValues.filter(
#     lambda oacfkv: str((oacfkv[1]).split(" ")[7]).lower() in ["opened",
#                                                               "closed"])  # total 9551 #dist keys 4819 #dist kval 9420
# openedAndClosedFilterKeyValuesDistinct = openedAndClosedFilterKeyValues.distinct()  # count 9420
# openedSession = openedAndClosedFilterKeyValuesDistinct.filter(
#     lambda os: str((os[1]).split(" ")[7]).lower() == "opened")  # count 4830 #dist keys 4819
# closedSession = openedAndClosedFilterKeyValuesDistinct.filter(
#     lambda cs: str((cs[1]).split(" ")[7]).lower() == "closed")  # total 4590
# clossedSessionKeys = closedSession.keys().collect()
# onlyClosedOpenSession = openedSession.filter(lambda ocop: ocop[0] in clossedSessionKeys)  # 4590
# onlyClosedOpenSessionAndClosedSessionUnion = closedSession.union(
#     onlyClosedOpenSession)  # total 9180 #dist keys 4579 #dist kval 9180
# sshdMatchingKeyValuesTimeoutFilter = sshdMatchingKeyValues.filter(
#     lambda smkvtf: str((smkvtf[1]).split(" ")[5]).lower() == "timeout")  # 0
#
# openedAndClosedKeyValue = onlyClosedOpenSessionAndClosedSessionUnion.map(
#     lambda oackv: (oackv[0], (oackv[1]).split(" ")[2]))  # 9180 #dist keys 4579
# sessionTimeUsage = openedAndClosedKeyValue.reduceByKey(lambda stu1, stu2: timeDifference(stu1, stu2))

# timeFormat = "%H:%M:%S"
# sortByTime = sshdMatchingKeys.reduceByKey(
#     lambda x, y: x if (str(x.split(" ")[2]).strftime(timeFormat) > str(y.split(" ")[2]).strftime(timeFormat)))  # 4817
#
# sortByTime = sshdMatchingKeys.reduceByKey(
#     lambda x, y: x if (datetime.strptime(x.split(" ")[2], timeFormat) > datetime.strptime(y.split(" ")[2], timeFormat)))
