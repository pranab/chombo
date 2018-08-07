#!/usr/bin/python

import os
import sys
from random import randint
import time
import uuid
import threading
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

numDays = int(sys.argv[1])
sampIntv = int(sys.argv[2])
secInDay = 24 * 60 * 60
secInHour = 60 * 60

serverList = ["server1", "server2"]
curTime = int(time.time())
pastTime = curTime - numDays * secInDay
sampTime = pastTime
usageDistr = [GaussianRejectSampler(30,12), GaussianRejectSampler(60,14), GaussianRejectSampler(70,16), GaussianRejectSampler(90,8)]

while(sampTime < curTime):
	secIntoDay = sampTime % secInDay
	hourIntoDay = secIntoDay / secInHour
	
	if ((hourIntoDay >= 0 and hourIntoDay < 8) or (hourIntoDay >= 20 and hourIntoDay <= 23)):
		hourGr = 0
	elif ((hourIntoDay >= 8 and hourIntoDay < 10) or (hourIntoDay >= 12 and hourIntoDay < 14)):
		hourGr = 1
	elif ((hourIntoDay >= 10 and hourIntoDay < 12) or (hourIntoDay >= 18 and hourIntoDay < 20)):
		hourGr = 2
	else:
		hourGr = 3
		
	for server in serverList:
		usage = usageDistr[hourGr].sample()
		if (usage < 0):
			usage = 0
		st = sampTime + randint(-1,1)
		print "%s,%d,%d" %(server, usage, st)
		
	sampTime = sampTime + sampIntv
	
	
