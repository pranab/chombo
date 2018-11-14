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


#key : user value: list of items

catBrands = {}
allItems = []
categories = []
brands = []


numDc = 5
numCat = 20
numBrands = 10
numPr = 1000

priceRange = (1.000, 14.0) 
invRange = (20, 300)
secInHour = 60 * 60
secInDay = 24 * secInHour

#initialize
def initialize(numBrands,numCat, numPr):
	for b in range(numBrands):
		brandId = genID(12)
		brands.append(brandId)
		
	for c in range(numCat):
		catId = genID(10)
		categories.append(catId)
		cbc = randint(1,5)
		cb = []
		for b in range(cbc):
			cb.append(selectRandomFromList(brands))		
		catBrands[catId] = cb
				
# initial		
def crInitial(numBrands,numCat, numPr):
	initialize(numBrands, numCat, numPr)
	tm = int(time.time()) - 5 * secInDay
	
	np = 0
	while np < numPr:
		cid = 	selectRandomFromList(categories)	
		for bid in catBrands[cid]:
			pid = genID(14)
			price = randomFloat(priceRange[0], priceRange[1])
			inv = randint(invRange[0], invRange[1])
			print "%s,%s,%s,%d,%.2f,%d" %(pid, cid, bid, inv, price, tm)
			np += 1
	
			
def crDcProds(baseFileName):
	dcId = genID(6)		
	for rec in fileRecGen(baseFileName):	
		print dcId + "," + rec
	
# update and insert
def upsert(dcFileName, percentMutate):
	updCnt = 0
	insCnt = 0
	cbList = []
	tm = int(time.time()) -  secInDay
	for rec in fileRecGen(dcFileName, ","):	
		cid = rec[2]
		bid = rec[3]
		cbList.append((cid, bid))
		if isEventSampled(percentMutate):
			if isEventSampled(90):
				#update
				if isEventSampled(92):
					rec[4] = str(randint(invRange[0], invRange[1]))
				else:
					newPrice = float(rec[5]) * randomFloat(1.02, 1.06)
					rec[5] = "%.2f" % newPrice
				tm += randint(2, 10)
				rec[6] = str(tm)
				r =  ",".join(rec)	
				print r
				updCnt += 1			
			else:
				#insert
				dcId = rec[0]
				pid = genID(14)
				cb = selectRandomFromList(cbList)
				cid = cb[0]
				bid = cb[1]
				inv = randint(invRange[0], invRange[1])
				price = randomFloat(priceRange[0], priceRange[1])
				tm += randint(2, 10)
				print "%s,%s,%s,%s,%d,%.2f,%d" %(dcId, pid, cid, bid, inv, price, tm)
				insCnt += 1
	#print "update count %d  insert count %d" %(updCnt,insCnt)
	
	
# delete	
def delete(dcFileName, percentMutate):
	for rec in fileRecGen(dcFileName):	
		if randomFloat(0, 100.0) > percentMutate:
			print rec	

######### main ########
if __name__== "__main__":	
	op = sys.argv[1]
		
	if op == "ini":
		if (len(sys.argv) > 2):
			numBrands = int(sys.argv[2])
			numCat = int(sys.argv[3])
			numPr = int(sys.argv[4])
			
		crInitial(numBrands,numCat, numPr)

	elif op == "din":
		baseFile = sys.argv[2]
		crDcProds(baseFile)	
	
	elif op == "upi":
		dcFile = sys.argv[2]
		percentMutate = int(sys.argv[3])
		upsert(dcFile, percentMutate)

	elif op == "del":
		dcFile = sys.argv[2]
		percentMutate = float(sys.argv[3])
		delete(dcFile, percentMutate)
				