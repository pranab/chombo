#!/usr/bin/python

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

numStores = int(sys.argv[1])
numProducts = int(sys.argv[2])
numXactions = int(sys.argv[3])
numCusts = int(sys.argv[4])

storeIds = []
productIds = []
prices = []
custIds = []

for i in range(0, numStores):
	storeIds.append(genID(6))
	
for i in range(0, numProducts):
	productIds.append(genID(8))	
	prices.append(uniform(2.0, 99.0))

for i in range(0, numCusts):
	custIds.append(genID(10))
	
currency = "UKP"	
startTime = time.time() - 1000000

for i in range(0, numXactions):
	xid = genID(12)
	custId = selectRandomFromList(custIds)
	storeId = selectRandomFromList(storeIds)
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
		print "%s,%s,%s,%s,%s,%d,%s,%s"  %(xid,custId,storeId,timeString,prod,quantity,amount,currency)

	
