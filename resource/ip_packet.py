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

packetCount = int(sys.argv[1])

ipAddressPairList = ["165.68.112.84,165.68.103.116", "165.68.75.105,165.68.65.106"]
window = 15 * 60
day = 24 * 60 * 60
curTime = int(time.time())
pastTime = curTime - 30 * day
pastTime = int((pastTime / window)) * window
nextTime = pastTime
countDistr = [
0,2,0,1,
3,7,9,11,
15,22,31,42,
49,56,69,82,
93,109,121,135,
148,159,172,184,
201,213,225,246,
259,271,285,299,
323,335,349,359,
371,383,391,406,
413,425,438,448,
421,402,390,376,
354,341,320,309,
282,271,258,242,
231,218,207,194,
180,165,151,142,
131,120,107,93,
78,65,53,41,
39,31,27,21,
17,12,9,6,
6,4,5,3,
3,3,2,1,
2,1,2,0,
3,1,0,2
]
timeOfDayDistr = NonParamRejectSampler(0, window, countDistr)
sizeDistr = [GaussianRejectSampler(5000,1500), GaussianRejectSampler(2000,600)]
pCount = 0
while (pCount < packetCount):
	withinDay = nextTime % day
	numPacket = int(timeOfDayDistr.dist(withinDay))
	if (numPacket > 0):
		packetTime = nextTime + randint(-100,100)
		timeDiff = int(window / numPacket)
		for j in range(0,numPacket):
			ipPair = randint(0,1)
			size = sizeDistr[ipPair].sample()
			print "%s,%d,%d" %(ipAddressPairList[ipPair],packetTime, size)
			packetTime = packetTime + timeDiff + randint(-100,100)
			pCount = pCount + 1
	nextTime = nextTime + window
	
