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
import org.chombo.util.BasicUtils

trait GeneralUtility {
	
  /**
 * @param keyFieldOrdinals
 * @return
 */
  def getKeyLen(keyFieldOrdinals:Option[Array[Integer]]) : Int = {
	  var keyLen = 0
	  keyFieldOrdinals match {
	    case Some(fields : Array[Integer]) => keyLen +=  fields.length
	    case None => 
	  }
	  keyLen
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
  
  
}