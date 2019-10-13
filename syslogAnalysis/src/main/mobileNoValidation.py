from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("mobile_no_validation").getOrCreate()

mobileNo = spark.read.csv("c:\\mobileNo.txt")
for i in mobileNo.take():
    print(i)


import re
from string import *

mobileNoWithAlphabets = '_123 asa 789'

addressToVerify ='info@emailhippo.com'
match = re.match('^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$', addressToVerify)

if match == None:
	print('Bad Syntax')
	raise ValueError('Bad Syntax')

print(re.sub('[^0-9]','', mobileNoWithAlphabets))

if len(re.sub('[^0-9]','', mobileNoWithAlphabets)) == 10:
    print("valid mobile no")
else:
    print("Not valid")

mobileNoWithAlphabets = mobileNoWithAlphabets.replace(' ', '')

for i in ascii_letters:
    mobileNoWithAlphabets = mobileNoWithAlphabets.replace(i, '')

print(mobileNoWithAlphabets)




