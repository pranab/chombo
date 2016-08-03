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


package org.chombo.spark.common

import org.chombo.util.Utility


object Record {
  var floatPrecision = 6
  
  def apply(size:Int) : Record = new Record(size)
  
  def apply(size:Int, record:Record) : Record = new Record(size, record)
  
}

/**
 * @author pranab
 *
 */
class Record(val size:Int) extends Serializable {
	val array = new Array[Any](size)
	var cursor:Int = 0
	
	/**
	 * @param size
	 * @param record
	 */
	def this(size:Int, record:Record) {
	  this(size)
	  Array.copy(record.array, 0, array, 0, record.size)
	}
	
	def getArray() :Array[Any] = array
	
	/**
	 * @param index
	 * @param strVal
	 * @return
	 */
	def addString(index:Int, strVal:String) : Record = {
	  array(index) = strVal
	  this
	}

	/**
	 * @param strVal
	 * @return
	 */
	def addString(strVal:String) : Record = {
	  array(cursor) = strVal
	  cursor += 1
	  this
	}

	/**
	 * @param index
	 * @param intVal
	 * @return
	 */
	def addInt(index:Int, intVal:Int) : Record = {
	  array(index) = intVal
	  this
	}
	
	/**
	 * @param index
	 * @param strVal
	 * @return
	 */
	def addInt(index:Int, strVal:String) : Record = {
	  addInt(index, strVal.toInt)
	}
	
	/**
	 * @param intVal
	 * @return
	 */
	def addInt(intVal:Int) : Record = {
	  array(cursor) = intVal
	  cursor += 1
	  this
	}
	
	/**
	 * @param strVal
	 * @return
	 */
	def addInt(strVal:String) : Record = {
	  addInt(strVal.toInt)
	}
	
	/**
	 * @param index
	 * @param intVal
	 * @return
	 */
	def addLong(index:Int, longVal:Long) : Record = {
	  array(index) = longVal
	  this
	}
	
	/**
	 * @param index
	 * @param strVal
	 * @return
	 */
	def addLong(index:Int, strVal:String) : Record = {
	  addLong(index, strVal.toLong)
	}
	
	/**
	 * @param intVal
	 * @return
	 */
	def addLong(longVal:Long) : Record = {
	  array(cursor) = longVal
	  cursor += 1
	  this
	}
	
	/**
	 * @param strVal
	 * @return
	 */
	def addLong(strVal:String) : Record = {
	  addLong(strVal.toLong)
	}
	

	/**
	 * @param index
	 * @param dblVal
	 * @return
	 */
	def addDouble(index:Int, dblVal:Double) : Record = {
	  array(index) = dblVal
	  this
	}

	/**
	 * @param index
	 * @param strlVal
	 * @return
	 */
	def addDouble(index:Int, strlVal:String) : Record = {
	  addDouble(index, strlVal.toDouble)
	}

	/**
	 * @param dblVal
	 * @return
	 */
	def addDouble(dblVal:Double) : Record = {
	  array(cursor) = dblVal
	  cursor += 1
	  this
	}
	
	/**
	 * @param strlVal
	 * @return
	 */
	def addDouble(strlVal:String) : Record = {
	  addDouble(strlVal.toDouble)
	}

	/**
	 * @param values
	 * @return
	 */
	def add(values:Any*) : Record = {
	  cursor = 0
	  for (value <- values) {
		array(cursor) = value
		cursor += 1
	  }
	  this
	}

	/**
	 * @param index
	 * @return
	 */
	def getString(index:Int) : String = {
	  array(index).asInstanceOf[String]
	}
	
	/**
	 * @return
	 */
	def getString() : String = {
	  val strVal = array(cursor).asInstanceOf[String]
	  cursor += 1
	  strVal
	}
	
	/**
	 * @param index
	 * @return
	 */
	def getInt(index:Int) : Int = {
	  array(index).asInstanceOf[Int]
	}
	
	/**
	 * @return
	 */
	def getInt() : Int = {
	  val intVal = array(cursor).asInstanceOf[Int]
	  cursor += 1
	  intVal
	}

	/**
	 * @param index
	 * @return
	 */
	def getLong(index:Int) : Long = {
	  array(index).asInstanceOf[Long]
	}
	
	/**
	 * @return
	 */
	def getLong() : Long = {
	  val longVal = array(cursor).asInstanceOf[Long]
	  cursor += 1
	  longVal
	}
	
	/**
	 * @param index
	 * @return
	 */
	def getDouble(index:Int) : Double = {
	  array(index).asInstanceOf[Double]
	}
	
	/**
	 * @return
	 */
	def getDouble() : Double = {
	  val dblVal = array(cursor).asInstanceOf[Double]
	  cursor += 1
	  dblVal
	}
	
	/**
	 * 
	 */
	def intialize() {
	  cursor = 0
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	override def hashCode() : Int = {
	  var hashCode = 0
	  array.foreach(a => hashCode += a.hashCode)
	  hashCode
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	override def equals(obj : Any) : Boolean = {
	  obj match { 
      	case that: Record => {
      	  var isEqual = true
      	  if (array.length == that.array.length) {
	      	  for(i <- 0 until array.length) {
	      	    isEqual &&= array(i).equals(that.array(i))
	      	  }
      	  } else {
      	    isEqual = false
      	  }
      	  isEqual
      	} 
      	case _ => false 
	  }
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	override def toString() : String = {
	  val stArray = array.map(a => {
	    if (a.isInstanceOf[Double]) {
	      Utility.formatDouble(a.asInstanceOf[Double], Record.floatPrecision)
	    } else {
	      a
	    }
	  })
	  stArray.mkString(",")
	}
}