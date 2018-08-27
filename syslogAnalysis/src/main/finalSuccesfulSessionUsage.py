from pyspark import SparkConf, SparkContext
import sys

# if len(sys.argv) != 2:
#     print("<runLocal> <input>")
#     sys.exit(0)
#
# if str(sys.argv[0]).lower() == "runlocal":
#     conf = SparkConf().setMaster("local[2]").setAppName("session usage")
#     conf.set("spark.broadcast.compress", "false")
#     conf.set("spark.shuffle.compress", "false")
#     conf.set("spark.shuffle.spill.compress", "false")
#     sc = SparkContext(conf=conf)
# else:
conf = SparkConf().setAppName("session usage").setMaster("local[2]")
sc = SparkContext(conf=conf)
# load data from file
secureLog = sc.textFile("c:\\data\\secureMixed.log")  # count - 1166245
# filter our sshd logs
sshd = secureLog.filter(lambda sf: (str(sf.split(" ")[4])[:4]).lower() == "sshd")  # 1018878
# filter out accepted session logs
acceptedSession = sshd.filter(lambda AS: str(AS.split(" ")[5]).lower() == "accepted")  # 4903
# creating key for acceptedSession 8-userName, 10-IP #Jul 29 gw02 sshd[13697]: 183.82.20.197
acceptedSessionMap = acceptedSession.map(lambda asm: (
    str(asm.split(" ")[0]) + " " + str(asm.split(" ")[1]) + " " + str(asm.split(" ")[3]) + " " + str(
        asm.split(" ")[4]) + " " + str(asm.split(" ")[10]), asm))  # distinct key 4427 # 4829
# filter out opened session logs
openedSession = sshd.filter(lambda os: str(os.split(" ")[7]).lower() == "opened")  # 4900
# creating key with session id for openedSession
openedSessionMap = openedSession.map(lambda osm: (
    str(osm.split(" ")[0]) + " " + str(osm.split(" ")[1]) + " " + str(osm.split(" ")[3]) + " " + str(
        osm.split(" ")[4]) + " " + str(osm.split(" ")[10]), osm))  # distinct key 4427 #4817 #4819 #4829
# filter out closed session logs
closedSession = sshd.filter(lambda cs: str(cs.split(" ")[7]).lower() == "closed")  # 4835
# creating keys for closedsession   #Jul 29 gw02 sshd[13697]: yuvankishore6
closedSessionMap = sshd.filter(lambda csm: (
    str(csm.split(" ")[0]) + " " + str(csm.split(" ")[1]) + " " + str(csm.split(" ")[3]) + " " + str(
        csm.split(" ")[4]) + " " + str(csm.split(" ")[10]), csm))
# filter out session timedout logs
# timeoutSession = sshd.filter(lambda ts: str(ts.split(" ")[5]).lower() == "timeout,")    #1305
# combining opened session logs and closed session logs
openedAndClosedSession = openedSession.union(closedSession)  # 9735
# creating key with session id for openedAndClosedSession
openedAndClosedSessionMap = openedAndClosedSession.map(
    lambda oacsm: (str(oacsm.split(" ")[4]), oacsm))  # distinct keys 4434
