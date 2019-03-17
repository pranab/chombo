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

op = sys.argv[1]
secInHour = 60 * 60
secInDay = 24 * secInHour
secInWeek = 7 * secInDay
secInQuarterDay = 6 * secInHour

if op == "usage":
	numDays = int(sys.argv[2])
	numCust = int(sys.argv[3])
	xactionIntv = int(sys.argv[4])
	
	usageDistr = [GaussianRejectSampler(60,10), GaussianRejectSampler(100,20), \
	GaussianRejectSampler(180,40), GaussianRejectSampler(300,60)]
	
	intervalDistr = GaussianRejectSampler(xactionIntv, 2)
	
	custList = []
	for i in range(numCust):
		custList.append(genID(10))

	curTime = int(time.time())
	pastTime = curTime - (numDays + 1) * secInDay
	xactionTime = pastTime
	
	while(xactionTime < curTime):
		custId = selectRandomFromList(custList)
		xactionId = genID(16)
		
		#customer segment 
		h = hash(custId)
		if (h < 0):
			h = -h
		cl = h % 4	
		
		amount = usageDistr[cl].sample()	
		
		#make one distribution skewed
		if (cl == 1):
			if amount < 60:
				amount *= 1.2
			else:
				amount *= 0.8
		print "%d,%s,%s,%d,%.2f" %(cl, custId, xactionId, xactionTime, amount)
		interval = int(intervalDistr.sample()) * 60
		xactionTime += interval
		
elif op == "loyalty":
	fileName = sys.argv[2]
	for rec in fileRecGen(fileName, ","):
		cluster = int(rec[0])
		custID = rec[1]
		if cluster == 0:
			loyalty = sampleBinaryEvents(("L", "M"), 80)
		elif cluster == 1:
			loyalty = sampleBinaryEvents(("L", "M"), 40)
		elif cluster == 2:
			loyalty = sampleBinaryEvents(("M", "H"), 70)
		else:
			loyalty = sampleBinaryEvents(("M", "H"), 10)
		
		print "1,%s,%s" %(custID, loyalty)


