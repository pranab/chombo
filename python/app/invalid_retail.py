#!/usr/bin/python

import os
import sys
from random import randint
import time
import uuid
from array import *
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

def genInvalidData(baseFile):
	invRowTh = 10
	storeIdTh = 25
	storeZipTh = 10
	prodCatTh = 50
	amountTh = 30
	count = 0
	
	for rec in fileRecGen(baseFile, ","):
		if isEventSampled(invRowTh):
			if isEventSampled(storeIdTh):
				extra = randint(1,3)
				rec[1] = rec[1] + genID(extra)
				count += 1
			if isEventSampled(storeZipTh):
				if isEventSampled(60):
					rec[2] = mutateString(rec[2], 1, "alpha")
				else:
					rec[2] = ""
				count += 1
			if isEventSampled(prodCatTh):
				rec[5] = mutateString(rec[5], 2, "alpha")
				count += 1
			if isEventSampled(amountTh):
				if isEventSampled(70):
					val = float(rec[7]) + randomFloat(500.0, 1000.0)
					val = "%.2f" %(val)
					rec[7] = val
				else:
					rec[7] = ""
				count += 1
		mrec = ",".join(rec)
		print mrec	
	#print count
	
	
if __name__ == "__main__":
	baseFile = sys.argv[1]
	genInvalidData(baseFile)

