#!/usr/bin/python

import os
import sys
from random import randint
from random import uniform
import time
import uuid
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *


def set_event_window(beg_year_time, prod_ids, ev):
	ev["prod_ids"] = prod_ids
	ev["beg_time"] = beg_year_time + randint(30, 300) * ms_in_day
	ev["end_time"] = ev["beg_time"] + randint(5, 15) * ms_in_day
	
def increase_quantity(quantity, cur_time, ev, increase):
	if cur_time > ev["beg_time"] and cur_time < ev["end_time"]:
		quantity = int(quantity * (100 + increase + randint(1,10)) / 100)
	return quantity
	

##################################
num_prod = int(sys.argv[1])
num_sells = int(sys.argv[2])
num_custs = int(sys.argv[2])

prods = []
ev1 = {}
ev2 = {}
ev1_prod_ids = set()
ev2_prod_ids = set()
cust_ids = []
ms_in_day = 24 * 60 * 60 * 1000
ms_in_year = 365 * ms_in_day

for i in range(0, num_prod):
	prod_id = genID(10)
	prod = {}
	prods.append(prod)	
	prod["id"] = prod_id
	prod["quantity"] = randint(1,5)
	if (randint(0,100) < 10):
		ev1_prod_ids.add(prod_id)
	if (randint(0,100) < 5):
		ev2_prod_ids.add(prod_id)

for i in range(0, num_custs):
	cust_ids.append(genID(12))
	
beg_year_time = (curTimeMs() / ms_in_year - 1) * ms_in_year
av_time_gap = ms_in_year / num_sells
cur_time = beg_year_time
set_event_window(beg_year_time, ev1_prod_ids, ev1)
set_event_window(beg_year_time, ev2_prod_ids, ev2)


for i in range(0, num_sells):
	cur_time += randint(0, 2 * av_time_gap)
	cust_id = selectRandomFromList(cust_ids)
	prod = selectRandomFromList(prods)
	prod_id =  prod["id"]
	quantity = sampleFromBase(prod["quantity"], randint(1,5))
	if quantity < 1:
		quantity = 1
	quantity = increase_quantity(quantity, cur_time, ev1, 30)
	quantity = increase_quantity(quantity, cur_time, ev2, 50)
	print "%s,%s,%d,%d"  %(cust_id,prod_id,quantity,cur_time)

	
	