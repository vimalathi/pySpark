from unittest import util
from pyspark import SparkContext, SparkConf
from datetime import datetime

conf = SparkConf().setAppName("test sample").setMaster("local[2]")
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
        return (secs/61) / 60
    else:
        secs = (b - a).total_seconds()
        return (secs/61) / 60

sampleLog = sc.textFile("c:\\data\\secureSample.txt")
#filter out opend and closed session logs
openedAndClosedFilter = sampleLog.filter(lambda oacf: str(oacf.split(" ")[7]).lower() in ["opened", "closed"])
# creating unique key line -> Jul 29 gw02 sshd[13697]: yuvankishore6
openedAndClosedKeyValue = openedAndClosedFilter.map(
    lambda osm: (osm.split(" ")[0] + " " + osm.split(" ")[1] + " " + osm.split(" ")[3] + " " + osm.split(" ")[4] + " " +
                 osm.split(" ")[10], osm.split(" ")[2]))
# for i in openedAndClosedKeyValue.collect():
#     print i
sessionTimeUsage = openedAndClosedKeyValue.reduceByKey(lambda stu1, stu2: timeDifference(stu1, stu2), 1)
sessionTimeUsage.collect()

