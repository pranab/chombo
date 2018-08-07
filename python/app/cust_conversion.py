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

referrers =["paid search", "organic search", "direct", "social"]
conv_delay_distr = {}
conv_delay_distr["campaign1"] = NonParamRejectSampler(0,1,60,80,90,99,86,70,56,46,34,22,14,8)
conv_delay_distr["campaign2"] = NonParamRejectSampler(0,1,92,99,88,70,62,50,42,32,24,16,8,4)

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
	