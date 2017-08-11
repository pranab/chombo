#!/usr/bin/python

import os
import sys
from random import randint
import time
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

num_users = int(sys.argv[1])
num_years_back = int(sys.argv[2])

sec_per_year = 365 * 24 * 60 * 60 
cur_time = int(time.time())
num_year = int(cur_time / sec_per_year)
time_beg_year = (num_year - num_years_back) * sec_per_year
#print time_beg_year

referrers =["paid search", "search"]
conv_delay_distr = {}
conv_delay_distr["campaign1"] = NonParamRejectSampler(0,1,0.60,0.80,1.00,0.80,0.65,0.53,0.44,0.36,0.28,0.20,0.14,0.10)
conv_delay_distr["campaign2"] = NonParamRejectSampler(0,1,0.80,1.00,0.90,0.80,0.72,0.66,0.60,0.54,0.48,0.42,0.36,0.30)

campaign = "campaign2"
if num_years_back > 2:
	campaign = "campaign1"

for i in range(num_users):	
	cust_id = genID(10)
	sign_up_time = time_beg_year + randint(0, sec_per_year)
	sign_up_date = time.strftime('%Y-%m-%d', time.localtime(sign_up_time))
	ref = selectRandomFromList(referrers)
	month_to_conv = conv_delay_distr[campaign].sample()
	print "%s,%s,%s,%d" %(cust_id,ref,sign_up_date,month_to_conv)
	