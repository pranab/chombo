#!/usr/bin/python

import os
import sys
from random import randint
import time
import uuid
import threading
sys.path.append(os.path.abspath("../lib"))
from util import *


#key : user value: list of items
catBrands = {}

allItems = []
categories = ["cell phone", "tablet", "laptop", "wearables"]
catBrands["cell phone"] =["apple", "samsung", "Nokia", "LG", "HTC"]
catBrands["tablet"] = ["samsung", "apple", "amazon kindle", "google chromo"] 
catBrands["laptop"] = ["lenovo", "hp", "acer", "asus", "thinkpad"]
catBrands["wearables"] = ["fitbit", "garmin", "jawbone", "misfit"]

catPrice = {"cell phone" : (150,80), "tablet" : (200,100), "laptop" : (500,200), "wearables" : (100,50)}

# load user, item and event file
def loadItems(existItemFile):
	file = open(existItemFile, 'r')

	#read file
	for line in file:
		line.strip()
		allItems.append(line)
		
	file.close()

def createNewItems(categories, catBrands, count, timeShift):
	modTime =  int(time.time()) - timeShift
	for i in range(0,count):
		it = genID(10)
		cat = selectRandomFromList(categories)
		brands = catBrands[cat]
		if (randint(0,9) < 4):
			brand = brands[0]
		else:
			brand = selectRandomFromList(brands)
		price = catPrice[cat][0] +  randint(0,catPrice[cat][1])
		inventory = randint(0, 100)
		modTime = modTime + randint(100, 5000)
		print "%s,%s,%s,%d,%d,%d" %(it, cat, brand, price, inventory, modTime)

def upsertItems(allItems, count, timeShift):
	step = len(allItems) / count - 1
	nextIndex = 0
	modTime =  int(time.time()) - timeShift
	for i in range(0,count):
		if (randint(0, 10) < 3):
			it = genID(10)
			cat = selectRandomFromList(categories)
			brands = catBrands[cat]
			if (randint(0,9) < 4):
				brand = brands[0]
			else:
				brand = selectRandomFromList(brands)
			price = catPrice[cat][0] +  randint(0,catPrice[cat][1])
			inventory = randint(0, 100)
			modTime = modTime + randint(100, 5000)
		else:
			nextIndex = nextIndex + step + randint(-2, 2)
			items = allItems[nextIndex].split(',')
			it = items[0]
			cat = items[1]
			brand = items[2]
			price = int(items[3]) 
			if (randint(0,10) < 3):
				price = price + randint(-2, 5)
			inventory = int(items[4]) + randint(-10, 10)
			if (inventory < 0):
				inventory = 0
			modTime = modTime + randint(100, 5000)
		print "%s,%s,%s,%d,%d,%d" %(it, cat, brand, price, inventory, modTime)
		
		

def deleteItems(allItems, count, timeShift):
	step = len(allItems) / count - 1
	nextIndex = 0
	modTime =  int(time.time()) - timeShift
	for i in range(0,count):
		nextIndex = nextIndex + step + randint(-2, 2)
		items = allItems[nextIndex].split(',')
		modTime = modTime + randint(100, 5000)
		print "%s,%s,%s,%s,%s,%d" %(items[0], items[1], items[2], items[3], items[4], modTime)
		
##########################################################################
op = sys.argv[1]
count = int(sys.argv[2])
timeShift = long(sys.argv[3]) * 60 * 60

if (op == "upsert"):	 
	existItemFile = sys.argv[4]
	loadItems(existItemFile)
	upsertItems(allItems, count, timeShift)
	
elif (op == "new"):	
	createNewItems(categories, catBrands, count, timeShift)

elif (op == "delete"):
	existItemFile = sys.argv[4]
	loadItems(existItemFile)
	deleteItems(allItems, count, timeShift)
	

