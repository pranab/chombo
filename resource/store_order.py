#!/usr/bin/python

import os
import sys
from random import randint
import time
sys.path.append(os.path.abspath("../lib"))
from util import *

allProducts = []
allStores = []

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

	
def createStoreOrders(allStores, allProducts):	
	for store in allStores:
		numProd = 70 + randint(-20, 20)
		prodSelected = set()
		orderID = genID(12)
		for i in range(0,numProd):
			prod =  selectRandomFromList(allProducts)
			while prod in prodSelected:
				prod =  selectRandomFromList(allProducts)
			prodSelected.add(prod)
			quantity = (abs(hash(store + prod)) % 5 + 2) * 10  + randint(-5, 5)
			if (randint(0,10) < 3):
				shipping = "express"
			else:
				shipping = "normal"
			print "%s,%s,%s,%d,%s" %(store, orderID, prod, quantity, shipping)
			
existProdFile = sys.argv[1]
existStoreFile = sys.argv[2]	
	
load(existProdFile, allProducts)
load(existStoreFile, allStores)
createStoreOrders(allStores, allProducts)
