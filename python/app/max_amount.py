#!/usr/bin/python

import os
import sys
from random import randint
from array import *
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

def genMaxAmountData(baseFile):
	for rec in fileRecGen(baseFile, ","):
		maxAm = float(rec[2]) * randint(16,22)
		mrec = ",".join(rec)
		print "%s,%.3f" %(mrec, maxAm)
		
if __name__ == "__main__":
	baseFile = sys.argv[1]
	genMaxAmountData(baseFile)
