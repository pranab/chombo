#!/usr/bin/python

import os
import sys
from random import randint
from array import *
sys.path.append(os.path.abspath("../lib"))
from util import *

def parseParam(config):
	params = {}
	items = config.split(",")
	for item in items:
		subItems = item.split(":")
		params[subItems[0]] = subItems[1]
	return params
	
if __name__ == "__main__":
	config = sys.argv[1]
	rec = sys.argv[2]
	ritems = rec.split(",")
	prodID = ritems[4]
	amStr = ritems[7]
	
	if not amStr:
		status = "invalid"
	else:
		amount = float(amStr)
		params = parseParam(config)
		prodFile = params["prodFile"]
		with open(prodFile, "r") as fp:
			for line in fp:	
				line = line[:-1]
				items = line.split(",")	
				if prodID == items[0]:
					maxAmount = float(items[3])
					#print "%.3f, %.3f" %(amount, maxAmount)
					if amount > maxAmount:
						status = "invalid"
					else:
						status = "valid"
					break
	print status