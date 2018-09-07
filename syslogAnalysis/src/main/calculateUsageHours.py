from pyspark import SparkContext, SparkConf
from datetime import datetime, timedelta, date
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


# returns time difference between session start and end time in seconds
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
        return secs
    else:
        secs = (b - a).total_seconds()
        return secs


months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]  # .index()
# monInNumber = input()
monInNumber = 7
dayOfMonth = 29
weekInNumber = 30


# this will extract the dates in the given week
def extract_dates(year, week):
    dates = []
    dt = date(year, 1, 1)
    if dt.weekday() > 3:
        dt = dt + timedelta(7 - dt.weekday())
    else:
        dt = dt - timedelta(dt.weekday())
    dlt = timedelta(days=(week - 1) * 7)
    for i in range(0, 7):
        Date = dt + dlt + timedelta(days=i)
        dates.append(str(Date))
    return dates


fulldateList = extract_dates(2018, 30)  # 30th week of the year 2018
# extracting date and month into separate list
dateList = []
monthList = []
for i in range(0, len(fulldateList)):
    dateList.append(fulldateList[i][8:])
    x = int(fulldateList[i][5:7])
    if months[x - 1] not in monthList:
        monthList.append(months[x - 1])

# --------------------------------------------------------------------------------------------------------------------
# unique session usage in seconds common key wise (key => "Jul 10 gw02 sshd[15273]: pramodluffy")
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
sessionUsageInSecondsBykey = openedClosedSessionKeyValue.reduceByKey(lambda stu1, stu2: timeDifference(stu1, stu2))
sessionUsageInSecondsBykey.persist()
# unique session usage in seconds
sessionUsageInSecondsBykey.collect()  # 4588
# --------------------------------------------------------------------------------------------------------------------
# average time per session per day
# extracting Month and date as key and calculated time as value
sessionUsageInSecondsDayByMap = sessionUsageInSecondsBykey.map(
    lambda oacsudbym: (oacsudbym[0].split(" ")[0] + " " + oacsudbym[0].split(" ")[1], oacsudbym[1]))
# aggigating useage time by date and count by date
sessionUsageTotalInSecondsByDay = sessionUsageInSecondsDayByMap.aggregateByKey((0.0, 0), (
    lambda totalTimeAndCount, element: (totalTimeAndCount[0] + element, totalTimeAndCount[1] + 1)),
                                                                               (lambda finalTotalTimeAndCount,
                                                                                       interTotalTimeAndCount: (
                                                                                   finalTotalTimeAndCount[0] +
                                                                                   interTotalTimeAndCount[0],
                                                                                   finalTotalTimeAndCount[1] +
                                                                                   interTotalTimeAndCount[1])))  # 22
# average time per session per day wise
averageUsagePerSessionPerDay = sessionUsageTotalInSecondsByDay.map(
    lambda aupspd: (aupspd[0], aupspd[1][0] / aupspd[1][1]))
# --------------------------------------------------------------------------------------------------------------------
# average time per session per month
# filtering records for the required month
sessionUsageInSecondsMonthFilter = sessionUsageInSecondsDayByMap.filter(
    lambda suismf: suismf[0].split(" ")[0] in months[monInNumber - 1])
sessionUsageInSecondsMonthByMap = sessionUsageInSecondsMonthFilter.map(
    lambda suismbm: (suismbm[0].split(" ")[0], suismbm[1]))
sessionUsageTotalInSecondsByMonth = sessionUsageInSecondsMonthByMap.aggregateByKey((0.0, 0), (
    lambda totalTimeAndCount, element: (totalTimeAndCount[0] + element, totalTimeAndCount[1] + 1)),
                                                                                   (lambda finalTotalTimeAndCount,
                                                                                           interTotalTimeAndCount: (
                                                                                       finalTotalTimeAndCount[0] +
                                                                                       interTotalTimeAndCount[0],
                                                                                       finalTotalTimeAndCount[1] +
                                                                                       interTotalTimeAndCount[1])))
# average time per session per month
averageUsagePerSessionPerMonth = sessionUsageTotalInSecondsByMonth.map(
    lambda aupspm: (aupspm[0], aupspm[1][0] / aupspm[1][1]))
# --------------------------------------------------------------------------------------------------------------------
# average time per session per week
sessionUsageInSecondsWeekFilter = sessionUsageInSecondsBykey.filter(
    lambda suiswf: suiswf[0].split(" ")[0] in monthList and suiswf[0].split(" ")[1] in dateList)  # 1484
sessionUsageInSecondsByWeekMap = sessionUsageInSecondsWeekFilter.map(
    lambda suisbwm: ("week" + str(weekInNumber), suisbwm[1]))
sessionUsageTotalInSecondsByWeek = sessionUsageInSecondsByWeekMap.aggregateByKey((0.0, 0), (
    lambda totalTimeAndCount, element: (totalTimeAndCount[0] + element, totalTimeAndCount[1] + 1)),
                                                                                 (lambda finalTotalTimeAndCount,
                                                                                         interTotalTimeAndCount: (
                                                                                     finalTotalTimeAndCount[0] +
                                                                                     interTotalTimeAndCount[0],
                                                                                     finalTotalTimeAndCount[1] +
                                                                                     interTotalTimeAndCount[1])))

# average time per session per week in seconds
averageUsagePerSessionPerWeek = sessionUsageTotalInSecondsByWeek.map(
    lambda aupspw: (aupspw[0], aupspw[1][0] / aupspw[1][1]))

# --------------------------------------------------------------------------------------------------------------------
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
