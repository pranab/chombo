#!/usr/bin/python

import os
import sys
from random import randint
import time
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

num_users = int(sys.argv[1])
num_calls = int(sys.argv[2])

area_codes = [ 408,607,336,267,646,760,615,980,828,385,941,305,971,510,574,620,507,540,206,262,847,  \
941,470,323,630,615,346,216, 920,903,423,614,440,419,832,678,608,678,571,248,321,301,630,719,209, \
770,615,971,937,703]
locations = ["37.733:-122.446", "37.123:-122.743", "36.845:-121.851", "36.182:-122.352", "38.931:-123.958"]
hour_of_day_distr = [GaussianRejectSampler(10,1), GaussianRejectSampler(14,1.8), GaussianRejectSampler(16,1.4), \
GaussianRejectSampler(20,.9), GaussianRejectSampler(6,1.2)]


phone_nums = []
user_profile = {}
for i in range(num_users):
	phone_num = "(" + str(selectRandomFromList(area_codes)) + ")" + str(genNumID(3)) + " " + str(genNumID(4))
	phone_nums.append(phone_num)
	tower = selectRandomFromList(locations)
	hour = selectRandomFromList(hour_of_day_distr[:3])
	user_profile[phone_num] = (tower, hour)
	
for i in range(num_calls):
	phone_num = selectRandomFromList(phone_nums)
	profile = user_profile[phone_num]
	if randint(0, 100) < 80:
		tower = profile[0]
		hours = int(profile[1].sample()) 
	else:
		tower = selectRandomFromList(locations)	
		hours = int(selectRandomFromList(hour_of_day_distr).sample()) 
	
	print "%s,%s,%d" %(phone_num, tower, hours)	


