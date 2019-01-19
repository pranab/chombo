#!/usr/bin/python

#!/usr/bin/python

# chombo-python: 
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
import time
sys.path.append(os.path.abspath("../lib"))
from util import *

#stats file 
types = [typeString, typeInt, typeInt]
(filePath, keyLen, samplesPerBin) = processCmdLineArgs(types, "usage: ./bw.py stas_file_path  key_length samples_per_bin")

#print "%s  %d %d" %(filePath, keyLen, samplesPerBin)
cntFld = keyLen + 3

count = 0
with open(filePath, "r") as fp:
	for line in fp:	
		items = line.split(",")
		size = len(items)
		#print "size %d" %(size)
		key = ",".join(items[:keyLen])
		count = int(items[cntFld])
		min = float(items[size - 2])
		max = float(items[size - 1])
		#print "key %s   count %d   min %.6f   max %.6f" %(key,count, min, max)
		numBin = count / samplesPerBin
		delta = max - min
		count += 1
		binWidth = (max - min) / numBin
		print "%s,%.3f" %(key, binWidth)
