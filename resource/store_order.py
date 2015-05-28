#!/usr/bin/python

import os
import sys
from random import randint
import time
sys.path.append(os.path.abspath("../lib"))
from util import *

allProducts = []
allStores = []
categories = ["general", "produce", "poultry", "dairy"]
zipCodes = ["95126", "95137", "94936", "95024", "94925"]

#create product data
def createProducts(count):
	for i in range(0,count):
		it = genID(10)
		cat = selectRandomFromList(categories)
		price = 100 +  randint(0,200)
		print "%s,%s,%d" %(it, cat, price)

#create store data
def createStores(count):
	for i in range(0,count):
		it = genID(8)
		cat = selectRandomFromList(zipCodes)
		print "%s,%s" %(it, cat)

# load item file
def load(existFile, idArray):
	file = open(existFile, 'r')

	#read file
	for line in file:
		line.strip()
		tokens = line.split(',')	
		item = tokens[0]
		idArray.append(item)
		
	file.close()

	
def createStoreOrders(allStores, allProducts, avNumProduct):	
	for store in allStores:
		numProd = avNumProduct + randint(-20, 20)
		prodSelected = set()
		orderID = genID(12)
		for i in range(0,numProd):
			prod =  selectRandomFromList(allProducts)
			while prod in prodSelected:
				prod =  selectRandomFromList(allProducts)
			prodSelected.add(prod)
			variance = randint(6,10)
			quantity = (abs(hash(store + prod)) % 5 + 3) * 10
			if (randint(0,10) < 7):
				quantity = quantity + randint(-variance, variance)
			else:
				variance = 2 * variance
				quantity = quantity + randint(-variance, variance)
			
			if (randint(0,10) < 3):
				shipping = "express"
			else:
				shipping = "normal"
			print "%s,%s,%s,%d,%s" %(store, orderID, prod, quantity, shipping)
			
##############################################################################
op = sys.argv[1]
			
if (op == "createOrders"):
	existProdFile = sys.argv[2]
	existStoreFile = sys.argv[3]	
	avNumProduct = int(sys.argv[4])
	
	load(existProdFile, allProducts)
	load(existStoreFile, allStores)
	createStoreOrders(allStores, allProducts, avNumProduct)
elif (op == "createProducts"):
	count = int(sys.argv[2])
	createProducts(count)
elif (op == "createStores"):
	count = int(sys.argv[2])
	createStores(count)
	
