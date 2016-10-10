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

num_users = int(sys.argv[1])
plans = ["standard", "standard plus", "super"]

doc_beg = """{
	"dataUsage" : ["""
acct_beg_template = """
		{
			"acctID" : "$acctID",
  			"usages" : [ """
usage_template = """
  			{
    			"plan" : "$plan",
    			"deviceID" : "$deviceID",
    			"used" : $used,
    			"startTime" : $startTime,
    			"endTime" : $endTime
  			}"""
acct_end = """
  			]
		}"""

doc_end = """
	]
}
"""

past_time_ms = curTimeMs() - 30 * 24 * 60 * 60 * 1000
max_start_time = 10 * 24 * 60 * 60 * 1000
min_duration = 10 * 1000
max_duration = 100 * 1000

doc = doc_beg
for i in range(num_users):
	acctID = genID(8)
	acct_beg = acct_beg_template.replace("$acctID", acctID)
	doc = doc + acct_beg
	
	num_usage_recs = randint(10, 20)
	plan = selectRandomFromList(plans)
	deviceID = genID(12)
	for j in range(num_usage_recs):
		used = randint(2000000, 9000000)
		start_time = past_time_ms + randint(0, max_start_time)
		end_time = start_time + randint(min_duration, max_duration)
		usage = usage_template.replace("$plan", plan)
		usage = usage.replace("$deviceID", deviceID)
		usage = usage.replace("$used", str(used))
		usage = usage.replace("$startTime", str(start_time))
		usage = usage.replace("$endTime", str(end_time))
		if (j < num_usage_recs - 1):
			doc = doc +  usage + ","
		else :
			doc = doc +  usage
	if (i < num_users - 1):	
		doc = doc + acct_end + ","
	else:
		doc = doc + acct_end
doc = doc + doc_end
		
print doc		
		
			
	

