from pyspark import SparkContext, SparkConf
from datetime import date, timedelta

conf = SparkConf().setAppName("session_count").setMaster("local[2]")
conf.set("spark.broadcast.compress", "false")
conf.set("spark.shuffle.compress", "false")
conf.set("spark.shuffle.spill.compress", "false")
sc = SparkContext(conf=conf)
# No. of unique users using labs in a day, week & month.
# u'Jul 29 03:32:28 gw02 sshd[13697]: pam_unix(sshd:session): session opened for user yuvankishore6 by (uid=0)'

months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]  # .index()
# monInNumber = input()
monInNumber = 7
dayOfMonth = 29


# this will extract the dates in a given week (1 - 52)
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

# extracting 30th week of the year 2018
fulldateList = extract_dates(2018, 30)
# extracting date and month into separate list
dateList = []
monthList = []
for i in range(0, len(fulldateList)):
    dateList.append(fulldateList[i][8:])
    x = int(fulldateList[i][5:7])
    if months[x - 1] not in monthList:
        monthList.append(months[x - 1])

secureLog = sc.textFile("c:\\data\\secureMixed2.log").persist()  # 1018875
# filtering ssh logs
sshdFilter = secureLog.filter(lambda sl: (str(sl.split(" ")[4])[:4] == "sshd"))  # 1018875
# filtering opened session logs from previous rdd
openedFilter = sshdFilter.filter(lambda ocf: str(ocf.split(" ")[7]).lower() == "opened")  # 4899
# -------------------------------------------------
# no of unique users using lab in a day
# filtering logs by date of a month
openedDayByFilter = openedFilter.filter(
    lambda odbf: (odbf.split(" ")[0] == months[monInNumber - 1]) and (odbf.split(" ")[1] == str(dayOfMonth)))  # 163
# extracting unique user names from previous rdd
uniqueUsersListByDay = openedDayByFilter.mapPartitions(lambda uulbd: (uulbd.split(" ")[10])).distinct()  # 39
# -------------------------------------------------
# no of unique users using lab in a month
# filtering logs by month
openedMonthByFilter = openedFilter.filter(lambda ombf: ombf.split(" ")[0] == months[monInNumber - 1])  # 4899
# extracting unique user names from previous rdd
uniqueUsersListByMonth = openedDayByFilter.map(lambda uulbm: (uulbm.split(" ")[10])).distinct()  # 39
# -------------------------------------------------
# no of unique users using lab in a week
# filtering logs by dates of the given week
openedWeekByFilter = openedFilter.filter(
    lambda owbf: (owbf.split(" ")[0] in monthList and owbf.split(" ")[1] in dateList))  # 1625
# extracting unique user names from previous rdd
uniqueUsersListByWeek = openedWeekByFilter.map(lambda uulbw: (uulbw.split(" ")[10])).distinct()   # 120
