/*
 * chombo: on spark
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


package org.chombo.common

/**
 * @author pranab
 *
 */
class Record(val size:Int) {
	val record = new Array[Any](size)
	var cursor:Int = 0
	
	def addString(index:Int, strVal:String) : Record = {
	  record(index) = strVal
	  this
	}

	def addString(strVal:String) : Record = {
	  record(cursor) = strVal
	  cursor += 1
	  this
	}

	def addInt(index:Int, intVal:Int) : Record = {
	  record(index) = intVal
	  this
	}
	
	def addInt(intVal:Int) : Record = {
	  record(cursor) = intVal
	  cursor += 1
	  this
	}
	
	def addDouble(index:Int, dblVal:Double) : Record = {
	  record(index) = dblVal
	  this
	}
	
	def addDouble(dblVal:Double) : Record = {
	  record(cursor) = dblVal
	  cursor += 1
	  this
	}
	
	def add(values:Any*) : Record = {
	  cursor = 0
	  for (value <- values) {
		record(cursor) = value
		cursor += 1
	  }
	  this
	}

	def getString(index:Int) : String = {
	  record(index).asInstanceOf[String]
	}
	
	def getString() : String = {
	  val strVal = record(cursor).asInstanceOf[String]
	  cursor += 1
	  strVal
	}
	
	def getInt(index:Int) : Int = {
	  record(index).asInstanceOf[Int]
	}
	
	def getInt() : Int = {
	  val intVal = record(cursor).asInstanceOf[Int]
	  cursor += 1
	  intVal
	}

	def getDouble(index:Int) : Double = {
	  record(index).asInstanceOf[Double]
	}
	
	def getDouble() : Double = {
	  val dblVal = record(cursor).asInstanceOf[Double]
	  cursor += 1
	  dblVal
	}
	
	def intialize() {
	  cursor = 0
	}
}