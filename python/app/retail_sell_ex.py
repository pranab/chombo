#!/usr/bin/python

#!/Users/pranab/Tools/anaconda/bin/python

# avenir-python: Machine Learning
# Author: Pranab Ghosh
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You may
# obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0 
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.


import os
import sys
from random import randint
from random import uniform
import time
import uuid
import threading
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

def genData():
	numStores = int(sys.argv[2])
	numProducts = int(sys.argv[3])
	numXactions = int(sys.argv[4])
	numCusts = int(sys.argv[5])
	numZips = int(sys.argv[6])
	numDays = int(sys.argv[7])
	
	storeIds = []
	productIds = []
	prices = []
	custIds = []
	zipCodes = []

	for i in range(numStores):
		storeIds.append(genID(6))
	
	for i in range(numProducts):
		productIds.append(genID(8))	
		prices.append(uniform(2.0, 99.0))

	for i in range(numCusts):
		custIds.append(genID(10))
	
	for i in range(numZips):
		zip = "9" + genNumID(4)
		#if randint(0, 100) < 20:
		#	zip = zip + "-" + genNumID(4)
		zipCodes.append(zip)
	
	custZip = dict()
	for i in range(numCusts):
		custZip[custIds[i]] = selectRandomFromList(zipCodes)
		
	zipStores = dict()
	stGroups = splitList(storeIds, numZips)
	for i in range(numZips):
		zipStores[zipCodes[i]] = stGroups[i]
	
	currency = "USD"	
	startTime = time.time() - numDays * 24 * 60 * 60 

	for i in range(numXactions):
		xid = genID(12)
		custId = selectRandomFromList(custIds)
		zip = custZip[custId]
		storeId = selectRandomFromList(zipStores[zip])
		startTime = startTime + randint(30, 500)
		localtime = time.localtime(startTime)
		timeString  = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
	
		numItems = randint(1, 5)
		for j in range(0, numItems):
			p = randint(0, len(productIds)-1)
			prod = productIds[p]
			quantity = randint(1,5)
			price = quantity * prices[p]
			amount = "{0:.2f}".format(price)
			monetaryAmt = currency + " " + amount
			print ("{},{},{},{},{},{},{},{}".format(xid,custId,storeId,zip,timeString,prod,quantity,monetaryAmt))

def insMissing():
	fileName = sys.argv[2]
	fldMissDistr = NonParamRejectSampler(0, 1, 30, 20, 10, 40, 10, 10, 20, 5)
	for rec in fileRecGen(fileName, ","):
		if isEventSampled(10):
			mfCnt = randint(1, 4)
			for i in range(mfCnt):
				fld = fldMissDistr.sample()
				rec[fld] = ""
		print(",".join(rec))
	
if __name__ == "__main__":
	op = sys.argv[1]
	if (op == "gen"):
		genData()
	elif (op == "insMiss"):
		insMissing()
	
