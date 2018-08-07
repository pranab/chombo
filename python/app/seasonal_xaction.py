#!/usr/bin/python

import os
import sys
from random import randint
from random import uniform
import time
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

num_products = int(sys.argv[1])
num_xactions = int(sys.argv[2])

products = []
seasonal_products = []
sec_in_year = 365 * 24 * 60 * 60
sec_in_week = 7 * 24 * 60 * 60

for i in range(0, num_products):
	peoduct_id = genID(8)
	amount = sampleUniform(20, 200)
	products.append((peoduct_id, amount))	
	p = sampleUniform(0, 100)
	if p < 10:
		seasonal_products.append(i)	
		
cur_time = time.time()	
sec_into_year = cur_time % sec_in_year
start_time = cur_time - sec_into_year - sec_in_year
cur_time = start_time
interval = sec_in_year / num_xactions
interval_dev = int(interval / 10)

# each transaction
for i in range(0, num_xactions):
	xid = genID(12)
	cart_size = sampleUniform(5, 10)
	week_into_year = (cur_time - start_time) / sec_in_week
	in_season = week_into_year > 30 and week_into_year < 36
	
	# each item in cart
	for j in range(0, cart_size):
		seasonal_prod = False
		if in_season:
			if sampleUniform(0, 100) < 60:
				pr_indx = selectRandomFromList(seasonal_products)	
				prod_price = products[pr_indx]
				seasonal_prod = True
				#print "seasonal product"
			else:
				prod_price = selectRandomFromList(products)
		else:
			prod_price = selectRandomFromList(products)
			
		prod = prod_price[0]
		amt = sampleFromBase(prod_price[1], int(prod_price[1]/10))
		if seasonal_prod:
			amt = int((amt * sampleUniform(140, 200)) / 100)
		print "%s,%s,%d,%d"  %(xid,prod,amt,cur_time)	
		
	cur_time = cur_time + sampleFromBase(interval, interval_dev)
			
