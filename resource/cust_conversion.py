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
print time_beg_year

referrers =["campaign", "search"]
conv_delay_distr = {"campaign" : NonParamRejectSampler(0,1,0.60,0.80,1.00,0.80,0.65,0.53,0.44,0.36,0.28,0.20,0.14,0.10), \
"search" : NonParamRejectSampler(0,1,0.8,0.9,1.0,0.75,0.6,0.5,0.4,0.32,0.26,0.20,0.16,0.12,0.08)}

if num_years_back < 3:
	conv_delay_distr["campaign"] = NonParamRejectSampler(0,1,0.80,1.00,0.90,0.80,0.72,0.66,0.60,0.54,0.48,0.42,0.36,0.30)
	conv_delay_distr["search"] = NonParamRejectSampler(0,1,0.8,0.92,1.0,0.78,0.58,0.52,0.4,0.34,0.26,0.18,0.16,0.10,0.08)

for i in range(num_users):	
	cust_id = genID(10)
	sign_up_time = time_beg_year + randint(0, sec_per_year)
	sign_up_date = time.strftime('%Y-%m-%d', time.localtime(sign_up_time))
	ref = selectRandomFromList(referrers)
	month_to_conv = conv_delay_distr[ref].sample()
	print "%s,%s,%s,%d" %(cust_id,ref,sign_up_date,month_to_conv)
	