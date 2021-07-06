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

import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import scala.collection.mutable.HashSet
import scala.reflect.ClassTag
import org.chombo.util.BasicUtils
import org.chombo.stats.HistogramStat

trait GeneralUtility {

  /**
  * @param keyFields
  * @return
  */
  def getKeyFieldOrdinals(keyFields:Option[java.util.List[Integer]]) : Option[Array[Integer]] = {
    keyFields match {
	     case Some(fields:java.util.List[Integer]) => Some(fields.asScala.toArray)
	     case None => None  
	}
  }
  
  
  /**
  * @param list
  * @return
  */
  def toOptionalIntegerArray(list:Option[java.util.List[Integer]]) : Option[Array[Integer]] = {
    list match {
	     case Some(list:java.util.List[Integer]) => Some(list.asScala.toArray)
	     case None => None  
	}
  }

  /**
  * @param list
  * @return
  */
  def toOptionalIntArray(list:Option[java.util.List[Integer]]) : Option[Array[Int]] = {
    list match {
	     case Some(list:java.util.List[Integer]) => Some(list.asScala.toArray.map(i => i.toInt))
	     case None => None  
	}
  }


  /**
  * @param list
  * @return
  */
  def toOptionalStringArray(list:Option[java.util.List[String]]) : Option[Array[String]] = {
    list match {
	     case Some(list:java.util.List[String]) => Some(list.asScala.toArray)
	     case None => None  
	}
  }

    /**
  * @param list
  * @return
  */
  def toStringArray(list:java.util.List[String]) : Array[String] = list.asScala.toArray

  
  /**
  * @param list
  * @return
  */
  def toIntArray(list:java.util.List[Integer]) : Array[Int] = list.asScala.toArray.map(i => i.toInt)

  /**
  * @param list
  * @return
  */
  def toIntegerArray(list:java.util.List[Integer]) : Array[Integer] = list.asScala.toArray
 
  /**
  * @param list
  * @return
  */
  def toOptionalIntList(list:Option[java.util.List[Integer]]) : Option[List[Int]] = {
    list match {
	     case Some(list:java.util.List[Integer]) => {
	       val sArray = list.asScala.toList.map(i => i.toInt)
	       Some(sArray)
	     }
	     case None => None  
	}
  }
  
  /**
  * @param list
  * @return
  */
  def toIntList(list:java.util.List[Integer]) : List[Int] = {
    list.asScala.toList.map(i => i.toInt)
  }

  /**
  * @param list
  * @return
  */
  def toIntegerList(list:java.util.List[Integer]) : List[Integer] = {
    list.asScala.toList
  }

  /**
  * @param arr
  * @return
  */
  def fromOptionalIntegerToIntArray(arr:Option[Array[Integer]]) : Option[Array[Int]] = {
    arr match {
	     case Some(a:Array[Integer]) => Some(a.map(i => i.toInt))
	     case None => None  
	  }
  }
  /**
  * @param list
  * @return
  */
  def toDoubleArray(list:java.util.List[java.lang.Double]) : Array[Double] = list.asScala.toArray.map(d => d.doubleValue())
  
  /**
  * @param keyFieldOrdinals
  * @return
  */
  def getKeyLen(keyFieldOrdinals:Option[Array[Integer]]) : Int = {
	  keyFieldOrdinals match {
	    case Some(fields : Array[Integer]) => fields.length
	    case None => 0
	  }
  }
  
  /**
  * @param arr
  * @return
  */
  def getOptinalArrayLength[T](arr:Option[Array[T]], defLen:Int = 0) : Int = {
	  arr match {
	    case Some(arr : Array[T]) => arr.length
	    case None => defLen
	  }
  }
  

  /**
  * @param list
  * @param defLen
  * @return
  */
  def getOptinalListLength[T](list:Option[List[T]], defLen:Int = 0) : Int = {
	  list match {
	    case Some(list : List[T]) => list.length
	    case None => defLen
	  }
  }
  
  /**
  * @param array
  * @param seasonality
  * @param baseLen
  * @return
  */
  def getOptinalArrayLength[T](arr:Option[Array[T]], seasonality:Boolean, baseLen:Int) : Int = {
	  var keyLen = arr match {
	    case Some(arr : Array[T]) => arr.length + baseLen
	    case None => baseLen
	  }
	  if (seasonality)
	    keyLen += 2
	  keyLen
  }
  
  /**
  * @param value
  */
  def createStringFieldRec(value:String) : Record =  {
    val rec = Record(1)
    rec.addString(value)
  }

  /**
  * @param value
  */
  def createIntFieldRec(value:Int) : Record =  {
    val rec = Record(1)
    rec.addInt(value)
  }

  /**
  * @param value
  */
  def createLongFieldRec(value:Long) : Record =  {
    val rec = Record(1)
    rec.addLong(value)
  }

  /**
  * @param value
  */
  def createDoubleFieldRec(value:Double) : Record =  {
    val rec = Record(1)
    rec.addDouble(value)
  }

  /**
  * @param fields
  * @param keyFieldOrdinals
  * @param rec
  */
  def populateFields(fields:Array[String], fieldOrdinals:Option[Array[Int]], rec:Record)  {
	  fieldOrdinals match {
	      case Some(fieldOrds : Array[Int]) => {
	    	  for (kf <- fieldOrds) {
	    		  rec.addString(fields(kf))
			  }
	      }
	      case None => 
	  }
   }
  
  /**
  * @param fields
  * @param keyFieldOrdinals
  * @param rec
  */
  def populateFields(fields:Array[String], fieldOrdinals:Array[Int], rec:Record)  {
	  for (kf <- fieldOrdinals) {
		  rec.addString(fields(kf))
	  }
   }

 /**
  * @param fields
  * @param numFields
  * @param rec
  */
  def populateFields(fields:Array[String], numFields:Int, rec:Record)  {
	  for (kf <- 0 to numFields - 1) {
		  rec.addString(fields(kf))
	  }
   }

 /**
  * @param fields
  * @param numFields
  * @param rec
  */
  def populateFields(fields:Array[String], fieldOffset:Int, numFields:Int, rec:Record)  {
	  for (kf <- 0 to numFields - 1) {
		  rec.addString(fields(fieldOffset + kf))
	  }
   }

  /**
  * @param fields
  * @param keyFieldOrdinals
  * @param rec
  * @param defKey
  */
  def populateFields(fields:Array[String], fieldOrdinals:Option[Array[Int]], rec:Record, defKey:String)  {
	  fieldOrdinals match {
	      case Some(fieldOrds : Array[Int]) => {
	    	  for (kf <- fieldOrds) {
	    		  rec.addString(fields(kf))
			  }
	      }
	      case None => rec.add(defKey)
	  }
   }
  
  /**
  * @param recOne
  * @param recTwo
  * @return
  */
  def combineWithUniqueFields(recOne:Record, recTwo:Record) : Record = {
    val uniqueItems = HashSet[Any]()
    recOne.initialize()
    while(recOne.hasNext()) {
      uniqueItems.add(recOne.getAny)
    }
    recTwo.initialize()
    while(recTwo.hasNext()) {
      uniqueItems += recTwo.getAny
    }
    
    val newRec = Record(uniqueItems.size)
    uniqueItems.foreach(i => newRec.add(i))
    newRec
  }

  /**
 * @param data
 * @param fieldDelimIn
 * @param keyDefined
 * @param keyLen
 * @param keyFieldOrdinals
 * @param seqFieldOrd
 * @return
 */
  def replTimestampWithSeq(data:RDD[String], fieldDelimIn:String, keyDefined:Boolean, keyLen:Int, 
      keyFieldOrdinals:Option[Array[Integer]], seqFieldOrd:Int) : RDD[String] = {
	  if (keyDefined) {
		  data.map(line => {
			   val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
			   val key = Record(keyLen)
	           Record.populateFields(fields, keyFieldOrdinals, key)
	           val seq = fields(seqFieldOrd).toLong
	           key.addLong(seq)
	           (key, line)
		  }).sortByKey(true).zipWithIndex.map(z => {
		     val line = z._1._2
		     val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		     fields(seqFieldOrd) = z._2.toString
		     fields.mkString(fieldDelimIn)
		  })
	  } else {
		  data.sortBy(line => {
		    val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		    fields(seqFieldOrd).toLong
		  }, true).zipWithIndex.map(z => {
		    val line = z._1
		    val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		    fields(seqFieldOrd) = z._2.toString
		    fields.mkString(fieldDelimIn)
		  })
	  }
  }
  
  /**
  * @param data
  * @return
  */
  def findAverageDouble(data:RDD[(Record, (Int, Double))]) : RDD[(Record, Double)] = {
    data.reduceByKey((v1, v2) => {
      val count = v1._1 + v2._1
      val sum = v1._2 + v2._2
      (count, sum)
	  }).mapValues(v => v._2 / v._1)
  }
  
  /**
  * @param data
  * @return
  */
  def findAverageInt(data:RDD[(Record, (Int, Int))]) : RDD[(Record, Int)] = {
    data.reduceByKey((v1, v2) => {
      val count = v1._1 + v2._1
      val sum = v1._2 + v2._2
      (count, sum)
	  }).mapValues(v => v._2 / v._1)
  }

  /**
  * @param data
  * @return
  */
  def findMinMaxDouble(data:RDD[(Record,(Double,Double))]) : RDD[(Record, (Double,Double))] = {
    data.reduceByKey((v1, v2) => {
	  val min = if (v1._1 < v2._1) v1._1 else v2._1
	  val max = if (v1._2 > v2._2) v1._2 else v2._2
    (min, max)
	  })
  }
  
  /**
  * @param data
  * @return
  */
  def findMinMaxInt(data:RDD[(Record,(Int,Int))]) : RDD[(Record, (Int,Int))] = {
    data.reduceByKey((v1, v2) => {
	    val min = if (v1._1 < v2._1) v1._1 else v2._1
	    val max = if (v1._2 > v2._2) v1._2 else v2._2
      (min, max)
	  })
  }
  
  /**
  * @param map
  * @param key
  * @param errMsg
  * @return
  */
  def getMapValue[K,V](map:scala.collection.Map[K,V], key:K, errMsg:String) : V = {
     val va = map.get(key) match {
       case Some(v) => v
       case None => BasicUtils.assertCondition(false, errMsg)
     }
     va.asInstanceOf[V]
  }
  
  /**
  * @param map
  * @param key
  * @param errMsg
  * @return
  */
  def getMapValue[K,V](map:scala.collection.immutable.Map[K,V], key:K, errMsg:String) : V = {
     val va = map.get(key) match {
       case Some(v) => v
       case None => BasicUtils.assertCondition(false, errMsg)
     }
     va.asInstanceOf[V]
  }

  /**
   * transform values of a map based on a function
  * @param m
  * @param k
  * @param f
  * @return
  */
  def updateMapValue[A, B](m: Map[A, B], k: A)(f: B => B) = m.updated(k, f(m(k))) 
  
  /**
  * updates all keys 
  * @param m
  * @param f
  * @return
  */
  def updateMapKeys[K, V, NK](m: Map[K, V], f: K => NK) : Map[NK, V] = {
    m.map(v => {
      val nk = f(v._1)
      (nk, v._2)
    })
  }
  
  /**
  * updates all values
  * @param m
  * @param f
  * @return
  */
  def updateMapValues[K, V, NV](m: Map[K, V], f: V => NV) : Map[K, NV] = {
    m.map(v => {
      val nv = f(v._2)
      (v._1, nv)
    })
  }

  /**
  * create Option from raw object 
  * @param obj
  * @return
  */
  def asOption[T:ClassTag](obj : T) : Option[T] = {
    if (null != obj) {
      Some(obj)
    } else {
      None
    }
  }

  /**
  * get raw array from option array
  * @param arr
  * @return
  */
  def filterNoneFromArray[T:ClassTag](arr : Array[Option[T]]) : Array[T] = {
      //remove None
	  val filtArr = arr.filter(a => {
	    a match {
	      case Some(x:T) => true
	      case None => false
	    }
	  })
		  
	  //peel off Some wrapper
	  val rawArr = filtArr.map(a => {
	    a match {
	      case Some(x:T) => x
	      case None => throw new IllegalStateException("no None exccepted")
	    }
	    
	  })
	rawArr 
  }
  
  /**
  * @param name
  * @return
  */
  def createInstance[T:ClassTag](name: String): T = {
    val cls =  Class.forName(name)
    val co = cls.getConstructors()(0)
    val obj = co.newInstance()
	  obj.asInstanceOf[T]
  }
  
  /**
  * @param data
  * @param seqFieldOrd
  * @return
  */
  def groupByKeySortBySeq(data:RDD[(Record, Array[String])], seqFieldOrd:Int, delim:String): RDD[String] = {
    data.groupByKey.flatMap(r => {
      val values = r._2.toArray.sortWith((v1, v2) => {
        v1(seqFieldOrd).toLong < v2(seqFieldOrd).toLong
      })
      values.map(r => r.mkString(delim))
    })
  }
  
  /**
  * @param values
  * @param index
  * @return
  */
  def getColumnAverage(values:Array[Array[String]], index:Int) : Double = {
	  val sum = values.map(v => v(index).toDouble).reduce((v1, v2) => v1 + v2)
	  sum / values.length
  }

  /**
  * @param values
  * @param index
  * @return
  */
  def getColumnAverage(values:RDD[Record], index:Int) : Double = {
    values.cache
	  val sum = values.map(v => v.getDouble(index)).reduce((v1, v2) => v1 + v2)
	  sum / values.count
  }

 /**
 * @param records
 * @param wtIndex
 * @param valIndex
 * @return
 */
 def getColumnWeightedAverage(records:Array[Record], wtIndex:Int, valIndex:Int) : Double = {
    val weights = records.map(v => v.getDouble(wtIndex))
	  val values = records.map(v => v.getDouble(valIndex))
	  var sum = 0.0
	  values.zip(weights).foreach(v => {sum +=  v._1 * v._2})
    sum / weights.sum
  }

 /**
 * @param records
 * @param wtIndex
 * @param valIndex
 * @return
 */
 def getColumnWeightedAverage(records:Array[Array[String]], wtIndex:Int, valIndex:Int) : Double = {
    val weights = records.map(v => v(wtIndex).toDouble)
	  val values = records.map(v => v(valIndex).toDouble)
	  var sum = 0.0
	  values.zip(weights).foreach(v => {sum +=  v._1 * v._2})
    sum / weights.sum
  }

 /**
  * @param values
  * @param index
  * @return
  */
  def getColumnStat(values:Array[Array[String]], index:Int) : (Double, Double) = {
    val col = values.map(v => v(index).toDouble)
	  val sum = col.reduce((v1, v2) => v1 + v2)
	  val sumSq = col.reduce((v1, v2) => v1 * v1 + v2 * v2)
	  val mean = sum / values.length
	  val sd = Math.sqrt(sumSq / (values.length - 1) - mean * mean)
	  (mean, sd)
  }

  /**
  * @param values
  * @param index
  * @return
  */
  def getColumnStat(values:RDD[Record], index:Int) : (Double, Double) = {
    values.cache
    val count = values.count
    val col = values.map(v => v.getDouble(index))
    col.cache
	  val sum = col.reduce((v1, v2) => v1 + v2)
	  val sumSq = col.reduce((v1, v2) => v1 * v1 + v2 * v2)
	  val mean = sum / count
	  val sd = Math.sqrt(sumSq / (count - 1) - mean * mean)
	  (mean, sd)
  }

  /**
  * @param values
  * @param index
  * @return
  */
  def getColumnMax(values:Array[Array[String]], index:Int) : Double = {
	  values.map(v => v(index).toDouble).reduce((v1, v2) => if (v1 > v2) v1 else v2)
  }
  
   /**
  * @param values
  * @param index
  * @return
  */
  def getColumnDistrStat(values:RDD[Record], index:Int, binWidth:Double) : HistogramStat = {
    val col = values.map(v => v.getDouble(index))
	  val distr = col.map(v => {
	    val hist = new HistogramStat(binWidth)
	    hist.add(v)
	    hist
	  }).reduce((h1, h2) => h1.merge(h2))
	  distr
  }
  
  /**
  * @param s
  * @return
  */
  def getDoubleSeqAverage(s: Seq[Double]): Double =  { 
    val sc = s.foldLeft((0.0, 0)) ((acc, v) => (acc._1 + v, acc._2 + 1)); 
    sc._1 / sc._2 
  }
  
  /**
  * @param items
  * @param keyFieldOrdinals
  * @param seqFieldOrd
  * @param quantFldOrd
  * @return
  */
  def getTimeSeriesKeyValue(items:Array[String], keyFieldOrdinals:Array[Int], seqFieldOrd:Int, quantFldOrd:Int) :
    (Record, Record) = {
    val keyRec = Record(items, keyFieldOrdinals)
    val valRec = Record(2)
    valRec.addLong(items(seqFieldOrd).toLong)
	  valRec.addDouble(items(quantFldOrd).toDouble)
    (keyRec, valRec)
  }

	/**
	 * @param data
	 * @param fieldDelimIn
	 * @param keyLen
	 * @param keyFieldOrdinals
	 * @param seqFieldOrd
	 * @param gen
	 * @return
	 */
	def getKeyedValueWithSeq(data: RDD[String], fieldDelimIn:String, keyLen:Int, 
	    keyFieldOrdinals: Option[Array[Int]], seqFieldOrd:Int) : RDD[(Record, Record)] =  {
	   data.map(line => {
	     val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	     val key = Record(keyLen)
	     //gen.populateFields(items, keyFieldOrdinals, key, "all")
	     
	     keyFieldOrdinals match {
	      case Some(fieldOrds : Array[Int]) => {
	    	  for (kf <- fieldOrds) {
	    		  key.addString(items(kf))
			    }
	      }
	      case None => key.add("all")
	    }


	     val value = Record(2)
	     val seq = items(seqFieldOrd).toLong
	     value.addLong(seq)
	     value.addString(line)
	     (key, value)
	   })	 
	  
	}

	/**
	 * @param data
	 * @param fieldDelimIn
	 * @param keyLen
	 * @param keyFieldOrdinals
	 * @param seqFieldOrd
	 * @param gen
	 * @return
	 */
	def getKeyedValue(data: RDD[String], fieldDelimIn:String, keyLen:Int, 
	    keyFieldOrdinals: Option[Array[Int]]) : RDD[(Record, Record)] =  {
	   data.map(line => {
	     val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	     val key = Record(keyLen)
	     //gen.populateFields(items, keyFieldOrdinals, key, "all")
	     
	     keyFieldOrdinals match {
	       case Some(fieldOrds : Array[Int]) => {
	    	   for (kf <- fieldOrds) {
	    		   key.addString(items(kf))
			     }
	       }
	       case None => key.add("all")
	     }

	     val value = Record(1)
	     value.addString(line)
	     (key, value)
	   })	 
	  
	}
	

/**
	 * @param data
	 * @param fieldDelimIn
	 * @param keyLen
	 * @return
	 */
	def getMandatoryKeyedValue(data: RDD[String], fieldDelimIn:String, keyLen: Int) : 
	RDD[(Record, String)] =  {
	   data.map(line => {
	     val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	     val key = Record(keyLen)
	     
	     for (i <- 0 to keyLen - 1) {
	       key.addString(items(i))
			 }
	     (key, line)
	   })	 
	  
	}

}