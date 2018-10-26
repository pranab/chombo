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
import scala.collection.mutable.Buffer
import org.chombo.util.BasicUtils
import org.chombo.util.BaseAttribute
import scala.reflect.ClassTag


object Record {
  var floatPrecision = 6
  
  /**
 * @param size
 * @return
 */
  def apply(size:Int) : Record = new Record(size)
  
  /**
 * @param size
 * @param record
 * @return
 */
  def apply(record:Record) : Record = new Record(record)

  /**
 * @param size
 * @param record
 * @return
 */
  def apply(size:Int, record:Record) : Record = new Record(size, record)

  /**
 * @param size
 * @param record
 * @return
 */
  def apply(size:Int, record:Record, offset:Int) : Record = new Record(size, record, offset)

  /**
   * @param record
   * @param beg
   * @param end
   * @return
  */
  def apply(record:Record, beg:Int, end:Int) : Record = new Record(record, beg, end)
  
  /**
   * @param data
   * @param beg
   * @param end
   * @return
  */
  def apply(data:Array[String], beg:Int, end:Int) : Record = new Record(data, beg, end)
  
  /**
   * @param data
   * @return
  */
  def apply(data:Array[String]) : Record = new Record(data)

  /**
   * @param size
   * @param data
   * @param beg
   * @param end
   * @return
  */
  def apply(size : Int, data:Array[String], beg:Int, end:Int) : Record = new Record(size, data, beg, end)

  /**
   * @param fields
   * @param fieldOrdinals
   * @return
  */
  def apply(fields: Array[String], fieldOrdinals: Array[Integer]) : Record = new Record(fields, fieldOrdinals)
  
  /**
   * @param size
   * @param fields
   * @param fieldOrdinals
   * @return
  */
  def apply(size: Int, fields: Array[String], fieldOrdinals: Array[Integer]) : Record = new Record(size, fields, fieldOrdinals)

  /**
   * @param size
   * @param intVal
   * @return
  */
  def apply(size:Int, intVal:Int) : Record = new Record(size, intVal)
  
  /**
   * @param strVal
   * @return
  */
  def apply(strVal:String) : Record = new Record(strVal)

  /**
   * @param fields
   * @param fieldOrdinals
   * @return
  */
  def extractFields(fields: Array[String], fieldOrdinals: Buffer[Integer]) : Record = {
	  val keyRec = new Record(fieldOrdinals.length)
	  fieldOrdinals.foreach(ord => {
	      keyRec.addString(fields(ord))
	  })
	  keyRec
  }
  
  /**
 * @param fields
 * @param beg
 * @param end
 * @return
 */
  def extractFields(fields: Array[String], beg:Int, end:Int) : Record = {
	  val rec = new Record(end-beg)
	  for(i <- beg to (end - 1)){
	     rec.addString(fields(i))
	  }
	  rec
  }
  
  /**
  * @param fields
  * @param keyFieldOrdinals
  * @param rec
  */
  def populateFields(fields:Array[String], fieldOrdinals:Option[Array[Integer]], rec:Record)  {
	  fieldOrdinals match {
	      case Some(fieldOrds : Array[Integer]) => {
	    	  for (kf <- fieldOrds) {
	    		  rec.addString(fields(kf))
			  }
	      }
	      case None =>
	  }
   }
 
   def setPrecision(floatPrecision : Int) {
     Record.floatPrecision = floatPrecision
   }
}

/**
 * @author pranab
 *
 */
class Record(val size:Int) extends Serializable with Ordered[Record]{
	val array = new Array[Any](size)
	var cursor:Int = 0
	var sortFields:Option[Array[Int]] = None
	var secondaryKeySize = 1
	var floatPrecision = -1
	
	/**
	 * @param size
	 * @param record
	 */
	def this(record:Record) {
	  this(record.size)
	  Array.copy(record.array, 0, array, 0, record.size)
	  cursor += record.size
	}
	
	/**
	 * @param size
	 * @param record
	 */
	def this(size:Int, record:Record) {
	  this(size)
	  Array.copy(record.array, 0, array, 0, record.size)
	  cursor += record.size
	}

	/**
	 * @param size
	 * @param record
	 * @param offset
	 */
	def this(size:Int, record:Record, offset:Int) {
	  this(size)
	  Array.copy(record.array, 0, array, offset, record.size)
	  cursor += (offset + record.size)
	}

	/**
 	* @param record
 	* @param beg
 	* @param end
 	*/	
	def this(record:Record, beg:Int, end:Int) {
	  this(end - beg)
	  val size = end - beg
	  Array.copy(record.array, beg, array, 0, size)
	  cursor += size
	} 
	
	/**
 	* @param data
 	* @param beg
 	* @param end
 	*/	
	def this(data:Array[String], beg:Int, end:Int) {
	  this(end - beg)
	  for(i <- beg to (end - 1)){
	     addString(data(i))
	  }
	} 

	/**
 	* @param data
 	*/	
	def this(data:Array[String]) {
	  this(data, 0, data.length)
	} 
	
	/**
 	* @param data
 	* @param beg
 	* @param end
 	*/	
	def this(size : Int, data:Array[String], beg:Int, end:Int) {
	  this(size)
	  for(i <- beg to (end - 1)){
	     addString(data(i))
	  }
	} 

	/**
	 * @param fields
	 * @param fieldOrdinals
	 */
	def this(fields: Array[String], fieldOrdinals: Array[Integer]) {
	  this(fieldOrdinals.length)
	  fieldOrdinals.foreach(ord => {
	      addString(fields(ord))
	  })
	}
	
	/**
	 * @param fields
	 * @param fieldOrdinals
	 */
	def this(size : Int, fields: Array[String], fieldOrdinals: Array[Integer]) {
	  this(size)
	  require(size > fieldOrdinals.length, "size should be greater than supplied fields length")
	  fieldOrdinals.foreach(ord => {
	      addString(fields(ord))
	  })
	}
	
	/**
	* @param intVal
	*/
	def this(size:Int, intVal:Int) {
	  this(1)
	  array(cursor) = intVal
	}
	
	/**
	* @param strVal
	*/
	def this(strVal:String) {
	  this(1)
	  array(cursor) = strVal
	}

	/**
	 * @param sortFields
	 */
	def withSortFields(sortFields : Array[Int]) : Record = {
	  this.sortFields = Some(sortFields)
	  this
	}
	
	def withSecondaryKeySize(secondaryKeySize : Int) : Record = {
	  this.secondaryKeySize = secondaryKeySize
	  this
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
	def addFloat(index:Int, fltVal:Float) : Record = {
	  array(index) = fltVal
	  this
	}

	/**
	 * @param index
	 * @param strlVal
	 * @return
	 */
	def addFloat(index:Int, strVal:String) : Record = {
	  addFloat(index, strVal.toFloat)
	}

	/**
	 * @param dblVal
	 * @return
	 */
	def addFloat(fltVal:Float) : Record = {
	  array(cursor) = fltVal
	  cursor += 1
	  this
	}
	
	/**
	 * @param strlVal
	 * @return
	 */
	def addFloat(strlVal:String) : Record = {
	  addFloat(strlVal.toFloat)
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
	 * @param index
	 * @param intVal
	 * @return
	 */
	def addBoolean(index:Int, boolVal:Boolean) : Record = {
	  array(index) = boolVal
	  this
	}
	
	/**
	 * @param index
	 * @param strlVal
	 * @return
	 */
	def addBoolean(index:Int, strlVal:String) : Record = {
	  addBoolean(index, strlVal.toBoolean)
	}	
	
	/**
	 * @param intVal
	 * @return
	 */
	def addBoolean(boolVal:Boolean) : Record = {
	  array(cursor) = boolVal
	  cursor += 1
	  this
	}

	/**
	 * @param strlVal
	 * @return
	 */
	def addBoolean(strlVal:String) : Record = {
	  addBoolean(strlVal.toBoolean)
	}

	/**
	 * @param strlVal
	 * @param fieldType
	 * @return
	 */
	def addStringAsTyped(strVal:String, fieldType:String) : Record = {
	  val rec = fieldType match {
	    case BaseAttribute.DATA_TYPE_STRING => addString(strVal)
	    case BaseAttribute.DATA_TYPE_INT => addInt(strVal)
	    case BaseAttribute.DATA_TYPE_LONG => addLong(strVal)
	    case BaseAttribute.DATA_TYPE_FLOAT => addFloat(strVal)
	    case BaseAttribute.DATA_TYPE_DOUBLE => addDouble(strVal)
	  }
	  rec
	}
	
	/**
	 * @param index
	 * @param strlVal
	 * @param fieldType
	 * @return
	 */
	def addStringAsTyped(index:Int, strVal:String, fieldType:String) : Record = {
	  val rec = fieldType match {
	    case BaseAttribute.DATA_TYPE_STRING => addString(index, strVal)
	    case BaseAttribute.DATA_TYPE_INT => addInt(index, strVal)
	    case BaseAttribute.DATA_TYPE_LONG => addLong(index, strVal)
	    case BaseAttribute.DATA_TYPE_FLOAT => addFloat(index, strVal)
	    case BaseAttribute.DATA_TYPE_DOUBLE => addDouble(index, strVal)
	  }
	  rec
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
	 * @param record
	 * @return
	 */
	def add(record:Record) : Record = {
	  Array.copy(record.array, 0, array, cursor, record.size)
	  cursor += record.size
	  this
	}
	
	/**
	 * @param record
 	 * @param beg
 	 * @param end
	 * @return
	 */
	def add(record:Record, beg : Int, end : Int) : Record = {
	  val size = end - beg
	  Array.copy(record.array, beg, array, cursor, size)
	  cursor += size
	  this
	}

	/**
	 * @param record
	 * @param indexes
	 * @return
	 */
	def add(record:Record, indexes : Array[Int]) : Record = {
	  indexes.foreach(i => {
	    array(cursor) = record.getAny(indexes(i))
		cursor += 1
	  })
	  this
	}


	/**
	 * @param index
	 * @return
	 */
	def getAny(index:Int) : Any = {
	  array(index)
	}
	
	/**
	 * @return
	 */
	def getAny() : Any = {
	  val anyVal = array(cursor)
	  cursor += 1
	  anyVal
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
	  hashCode = if (hashCode < 0) -hashCode else hashCode
	  hashCode
	}
	
	/**
	 * @return
	 */
	def baseHashCode() : Int = {
	  var hashCode = 0
	  for (i <- 0 to (size - 1 - secondaryKeySize)) {
	    hashCode += array(i).hashCode
	  }
	  hashCode = if (hashCode < 0) -hashCode else hashCode
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
	  toString(",")
	}
	
	/**
	 * @param delim
	 * @return
	 */
	def toString(delim : String) : String = {
	  val stArray = array.map(a => {
	    if (a.isInstanceOf[Double]) {
	      val precision = if (this.floatPrecision > 0)   this.floatPrecision else Record.floatPrecision
	      BasicUtils.formatDouble(a.asInstanceOf[Double], precision)
	    } else {
	      a
	    }
	  })
	  stArray.mkString(delim)
	}

	/**
	 * @param beg
	 * @param end
	 * @return
	 */
	def toString(beg:Int, end:Int) : String = {
	  toString(beg, end, ",")
	}

	/**
	 * @param beg
	 * @param end
	 * @param delim
	 * @return
	 */
	def toString(beg:Int, end:Int, delim : String) : String = {
	  val sarray = array.slice(beg, end)
	  val stArray = sarray.map(a => {
	    if (a.isInstanceOf[Double]) {
	      val precision = if (this.floatPrecision > 0)   this.floatPrecision else Record.floatPrecision
	      BasicUtils.formatDouble(a.asInstanceOf[Double], precision)
	    } else {
	      a
	    }
	  })
	  stArray.mkString(delim)
	}

	/**
	 * @param that
	 * @return
	 */
	def compare(that: Record): Int = {
	  this.array.length compareTo that.array.length match { case 0 => 0; case c => return c }
	  
	  sortFields match {
	    case Some(sFields:Array[Int]) => {
	      //sort by selected fields
	      for (i <- 0 to (sFields.length -1)) {
	    	  val thisEl = this.array(sFields(i))
	    	  val thatEl = that.array(sFields(i))
	    	  compareElement(thisEl, thatEl) match {
	    	  	case 0 => 0
	    	  	case c => return c
	    	  }
	      }
	    }
	    
	    case None => {
	    	//sort by all fields
	    	for (i <- 0 to (this.array.length -1)) {
	    		val thisEl = this.array(i)
	    		val thatEl = that.array(i)
	    		compareElement(thisEl, thatEl) match {
	    			case 0 => 0
	    			case c => return c
	    		}
	    	}
	    }
	  }
	  
	  0
	}
	
	/**
	 * @param thisEl
	 * @param thatEl
	 * @return
	 */
	def compareElement(thisEl:Any, thatEl:Any) : Int = {
	    if (thisEl.isInstanceOf[String] ) {
	      if (thatEl.isInstanceOf[String]) {
	    	thisEl.asInstanceOf[String] compareTo thatEl.asInstanceOf[String] match { case 0 => 0; case c => return c }
	      } else {
	        return 1
	      }
	    }
	    
	    if (thisEl.isInstanceOf[Int] ) {
	      if (thatEl.isInstanceOf[Int]) {
	    	thisEl.asInstanceOf[Int] compareTo thatEl.asInstanceOf[Int] match { case 0 => 0; case c => return c }
	      } else {
	        return 1
	      }
	    }

	    if (thisEl.isInstanceOf[Long] ) {
	      if (thatEl.isInstanceOf[Long]) {
	    	thisEl.asInstanceOf[Long] compareTo thatEl.asInstanceOf[Long] match { case 0 => 0; case c => return c }
	      } else {
	        return 1
	      }
	    }
	    
	    if (thisEl.isInstanceOf[Double] ) {
	      if (thatEl.isInstanceOf[Double]) {
	    	thisEl.asInstanceOf[Double] compareTo thatEl.asInstanceOf[Double] match { case 0 => 0; case c => return c }
	      } else {
	        return 1
	      }
	    }
	    0
	}
	
	/**
	 * @param suffixLen
	 * @return
	 */
	def prefixHashCode(suffixLen : Int) : Int = {
	  var hashCode = 0
	  for (i <- 0 to (array.length - suffixLen - 1)) {
	   hashCode += array(i).hashCode
	  }
	  hashCode = if (hashCode < 0) -hashCode else hashCode
	  hashCode
	}
	
	/**
	 * @param floatPrecision
	 * @return
	 */
	def withFloatPrecision(floatPrecision:Int) : Record =  {
	  this.floatPrecision = floatPrecision
	  return this
	}
	
}