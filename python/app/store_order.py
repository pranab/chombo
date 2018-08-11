#!/usr/bin/python

import os
import sys
from random import randint
import time
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

allProducts = []
allStores = []

categories = ["general", "produce", "poultry", "dairy", "beverages", "bakery", "canned food", \
"frozen food", "meat", "cleaner", "paper goods"]

catPriceRange = {"general" : (1.0, 20.0), "produce" : (1.0, 8.0), "poultry" : (2.0, 5.0), \
"dairy" : (1.0, 7.0), "beverages" : (4.0, 15.0), "bakery": (2.0, 8.0), "canned food" : (1.0, 3.0), "frozen food" : (2.0, 6.0), \
"meat" : (1.0, 6.0), "cleaner" : (1.0, 4.0), "paper goods" : (1.0, 3.0)}

zipCodes = ["95126", "95137", "94936", "95024", "94925", "94501", "94502", "94509", "94513", \
"94531", "94561", "94565", 	"94541", "94546", "94552", "94577", "94578"]

#create product data
def createProducts(count):
	for i in range(0,count):
		it = genID(10)
		cat = selectRandomFromList(categories)
		prRange = catPriceRange[cat]
		price = randomFloat(prRange[0], prRange[1])
		print "%s,%s,%.2f" %(it, cat, price)

#create store data
def createStores(count):
	for i in range(0,count):
		it = genID(8)
		zip = selectRandomFromList(zipCodes)
		print "%s,%s" %(it, zip)

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
			

def createReatilOrders(storesFile, productsFile, numOrders, numDays, numStores):	
	#products
	allProducts = []
	for prod in fileRecGen(productsFile, ","):
		allProducts.append(prod)
		
	#stores
	allStores = []
	for store in fileRecGen(storesFile, ","):
		allStores.append(store)
	
	daysInSec = 24 * 60 * 60
	ts = time.time() - 10 * daysInSec
	ordersByDay = distrUniformWithRanndom(numOrders, numDays, 0.05)
	ordersByDay = asIntList(ordersByDay)	
	#print "total orders " + str(ordersByDay)
	
	#days
	for i in range(numDays):
		t = time.localtime(ts)
		dt = "%d-%02d-%02d" %(t.tm_year, t.tm_mon, t.tm_mday)
		ts += daysInSec
		ordersByStore = distrUniformWithRanndom(ordersByDay[i], numStores, 0.10)
		ordersByStore = asIntList(ordersByStore)	
		#print "orders by store " + str(ordersByStore)
		
		#stores
		for j in range(numStores):
			store = allStores[j]
			
			#orders
			for k in range(ordersByStore[j]):
				orderID = genID(12)
				numItems = randint(1,20)
				
				#line items
				for l in range(numItems):
					prod = selectRandomFromList(allProducts)
					quantity = (abs(hash(store[0] + prod[0])) % 3 + 1)
					variance = randint(1,3)
					quantity = quantity + randint(-variance, variance)
					if quantity < 1:
						quantity = 1
					amount = quantity * float(prod[2])
					print "%s,%s,%s,%s,%s,%s,%d,%.2f" %(dt, store[0], store[1], orderID, prod[0], prod[1], quantity, amount)
		

if __name__ == "__main__":
	op = sys.argv[1]
			
	if op == "createOrders":
		existProdFile = sys.argv[2]
		existStoreFile = sys.argv[3]	
		avNumProduct = int(sys.argv[4])
		load(existProdFile, allProducts)
		load(existStoreFile, allStores)
		createStoreOrders(allStores, allProducts, avNumProduct)
	elif op == "createRetOrders":
		productsFile = sys.argv[2]
		storesFile = sys.argv[3]	
		numOrders = int(sys.argv[4])
		numDays = int(sys.argv[5])
		numStores = int(sys.argv[6])
		createReatilOrders(storesFile, productsFile, numOrders, numDays, numStores)
	elif op == "createProducts":
		count = int(sys.argv[2])
		createProducts(count)
	elif op == "createStores":
		count = int(sys.argv[2])
		createStores(count)
	else:
		print "invalid command"
	
