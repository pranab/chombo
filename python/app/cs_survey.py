#!/usr/bin/python

import os
import sys
from random import randint
import time
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

num_surveys = int(sys.argv[1])

cust_types = ["business", "residence"]
time_of_day =["AM", "PM"]
hold_time_distr = {"AM" : GaussianRejectSampler(600,100), "PM" : GaussianRejectSampler(300,60)}
num_prev_calls_distr = GaussianRejectSampler(1, 1.5)
csr_friendly_distr = GaussianRejectSampler(6,1.5)
veri_process_distr = GaussianRejectSampler(5 ,1.2)

with_missing_value = True

def insert_missing(record):
	if (with_missing_value and (randint(0, 100) < 10)):
		if (randint(0, 100) < 50):
			record[5] = ""
		if (randint(0, 100) < 60):
			record[6] = ""
		if (randint(0, 100) < 20):
			field = randint(7, 9)
			record[field] = ""
		
	if 	(randint(0, 100) < 70):
		record[6] = ""
	return record
			
for i in range(num_surveys):
	cust_id = genID(12)
	cust_type = selectRandomFromList(cust_types)
	tod = selectRandomFromList(time_of_day)
	hold_time = int(hold_time_distr[tod].sample())
	
	num_prev_calls = int(num_prev_calls_distr.sample())
	num_prev_calls = rangeLimit(num_prev_calls, 0, 5)

	num_reroute = randint(0, 3)
	
	cs_friendly_score = int(csr_friendly_distr.sample())
	cs_friendly_score = rangeLimit(cs_friendly_score, 1, 10)
	
	veri_process_score = int(veri_process_distr.sample())
	veri_process_score = rangeLimit(veri_process_score, 1, 10)
	
	if (num_prev_calls > 1):
		if (randint(0, 100) < 80):
			resolved = "T"
		else:
			resolved = "F"
	else:
		if (randint(0, 100) < 60):
			resolved = "T"
		else:
			resolved = "F"
	
	if (resolved == "T"):
		satisfac_with_csr = randint(5, 10)
	else:
		satisfac_with_csr = randint(1, 5)

	if (resolved == "T"):
		if (num_prev_calls < 2):
			satisfaction_overall = 	randint(7, 10)
		else: 	
			satisfaction_overall = 	randint(5, 7)
	else:
		if (num_prev_calls > 1):
			satisfaction_overall = 	randint(1, 3)
		else:
			satisfaction_overall = 	randint(3, 5)
		
	record = []	
	record.append(cust_id)	
	record.append(cust_type)	
	record.append(str(hold_time))	
	record.append(str(num_prev_calls))	
	record.append(str(num_reroute))	
	record.append(str(cs_friendly_score))	
	record.append(str(veri_process_score))	
	record.append(resolved)	
	record.append(str(satisfac_with_csr))	
	record.append(str(satisfaction_overall))	
	
	record = insert_missing(record)
	print ",".join(record)
		
