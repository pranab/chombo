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

catBrandModels={}

catPrice = {"cell phone" : (150,80), "tablet" : (200,100), "laptop" : (500,200), "wearables" : (100,50)}
catQuantity = {"cell phone" : (100,50), "tablet" : (80,20), "laptop" : (50,10), "wearables" : (20,10)}


def createItems(itemCount):
	for i in range(0,itemCount):
		allItems.append(genID(10))
		
def createModels():
	for c in categories:
		for b in catBrands[c]:
			numModel = 5 + randint(0,5)
			models = []
			for i in range(0,numModel):
				models.append(genID(6))
			catBrandModels[c + ":" + b] = models
		
def itemCatBrand(allItems, categories, catBrands, catQuantity, catBrandModels):
	for it in allItems:
		cat = selectRandomFromList(categories)
		brands = catBrands[cat]
		if (randint(0,9) < 4):
			brand = brands[0]
		else:
			brand = selectRandomFromList(brands)
		models = catBrandModels[cat + ":" + brand]
		model = selectRandomFromList(models)
		price = catPrice[cat][0] +  randint(0,catPrice[cat][1])
		quantity = catQuantity[cat][0] +  randint(0,catQuantity[cat][1])
		print "%s,%s,%s,%s,%d,%d" %(it, cat, brand, model, price, quantity)
		
##########################################################################

itemCount = int(sys.argv[1])
createItems(itemCount)
createModels()
itemCatBrand(allItems, categories, catBrands, catQuantity, catBrandModels)

