#!/usr/bin/python

import os
import sys
from random import randint
import time
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

num_houses = int(sys.argv[1])

#id, zip code, sq ft, num of bed rooms, num of bath, price

zip_codes = ["95129", "94501", "94502", "94536", "94537", "94538", "94539", "94540", "94541", \
"94550", "94555", "94566", "94601", "94602", "94701", "94702", "94703", "94704"]
high_cost_zip_codes = {"94701", "94702", "94703", "94704"}

for i in range(num_houses):
	id = genID(10)
	zip_code = selectRandomFromList(zip_codes)
	num_rooms = 3
	if (randint(0, 100) > 40):
		num_rooms = randint(3, 5)
	num_baths = 2
	if num_rooms == 3:
		fl_area = randint(1300, 1500)
	elif num_rooms == 4:
		if (randint(0, 100) > 70):
			num_baths = 3
		fl_area = randint(1500, 1700)
	elif num_rooms == 5:
		if (randint(0, 100) > 40):
			num_baths = 3
		fl_area = randint(1700, 2000)
	
	if num_baths == 3:
		fl_area = fl_area + 80
		
	price = fl_area * 680
	if zip_code in high_cost_zip_codes:
		price = fl_area * 800
	low = price - price / 20
	high = price + price / 20
	price = randint(low, high)
	price = (price / 1000) * 1000
	
	print "%s,%s,%d,%d,%d,%d" %(id, zip_code, fl_area, num_rooms, num_baths, price)	
	
