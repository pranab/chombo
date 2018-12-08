/*
 `* chombo-spark: etl on spark
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

package org.chombo.spark.explore

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import java.text.SimpleDateFormat
import org.chombo.spark.common.GeneralUtility

/**
 * Range partitions. Adds new field with partition index
 * @author pranab
 *
 */
object RangePartitioner extends JobConfiguration with GeneralUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "rangePartitioner"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = getKeyFieldOrdinals(keyFields)
	   
	   val seqFieldOrdinal = getMandatoryIntParam(appConfig, "seq.fieldOrdinal", 
	       "missing seq field ordinal")
	   val numPartitions = getOptionalIntParam(appConfig, "num.partitions")
	   val dateFormatStr = getOptionalStringParam(appConfig, "date.formatStr")
	   val dateFormat = dateFormatStr match {
	     case Some(date) => Some(new SimpleDateFormat(date))
	     case None => None
	   }
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)

	   val keyLen = keyFieldOrdinals match {
	     case Some(fields : Array[Integer]) =>  fields.length
	     case None => 0
	   }

	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   val partData = numPartitions match {
	     case Some(numPart) => {
	       //key wise or global based on number of partitions
	       val minRange = getMandatoryDoubleParam(appConfig, "range.min", "missing min range")
		   data.cache
		   
	       //min max
		   val dataRange = data.map(line => {
		    val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		    val seq = getPartField(fields(seqFieldOrdinal), dateFormat)
		    val key = buildKey(keyLen, fields, keyFieldOrdinals)
		    (key, (seq, seq))
		   }).reduceByKey((v1, v2) => {
		     val min = if (v1._1 < v2._1) v1._1 else v2._1
		     val max = if (v1._2 > v2._2) v1._2 else v2._2
		     (min, max)
		   })
		   
		   //partition size
		   val partSize = dataRange.mapValues(r => {
		     val range = r._2 - r._1
		     if (range > minRange) {
		       //normal
		       val size = range / numPart
		       (r._1, r._2, size, true)
		     } else {
		       // range too small
		       (r._1, r._2, range, false)
		     }
		   }).collectAsMap
		   
		   //partition
		   val partData = data.map(line => {
		     val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		     val seq = getPartField(fields(seqFieldOrdinal), dateFormat)
		     val key = buildKey(keyLen, fields, keyFieldOrdinals)
		     val ptSize = partSize.get(key)
		     val ptIndex = ptSize match {
		       case Some(part) => {
		         if (part._4) {
		           var ptIndex = (seq / part._3).floor.toInt
		           ptIndex = BasicUtils.between(ptIndex, 0, numPart - 1)
		           ptIndex
		         } else {
		           0
		         }
		       }
		       case None => throw new IllegalStateException("missing partition information")
		     }
		     ptIndex.toString + fieldDelimOut + line
		   }) 
		   partData
	     }
	   
	     case None => {
	       //global based on specified partition boundaries
	       val partBoundaries = getMandatoryDoubleListParam(appConfig, "part.boundaries", "missing range definitions").
	           asScala.toList.zipWithIndex
		   val partData = data.map(line => {
		    val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		    val seq = getPartField(fields(seqFieldOrdinal), dateFormat)
		    val range = partBoundaries.find(r => seq < r._1)
		    range match {
		      case Some(r) => r._2.toString + fieldDelimOut + line
		      case None => partBoundaries.length.toString + fieldDelimOut + line
		    }
		   }) 
		   partData
	     }
	   }
	   
	   if (debugOn) {
	     partData.collect.slice(0,50).foreach(s => println(s))
	   }
	   
	   if (saveOutput) {
	     partData.saveAsTextFile(outputPath)
	   }
	  
   }
   
   /**
   * @param config
   * @param paramName
   * @param defValue
   * @param errorMsg
   * @return
   */   
   def buildKey( keyLen:Int, fields:Array[String], keyFieldOrdinals: Option[Array[Integer]]) : Record = {
      if (keyLen > 0) {
	      val key = Record(keyLen)
		  Record.populateFields(fields, keyFieldOrdinals, key)
	      key
	  } else {
	      val key = Record(1)
	      key.add("fake")
	      key
	  }
   }
   
   /**
   * @param value
   * @param dateFormat
   * @return
   */
   def getPartField(value:String, dateFormat: Option[java.text.SimpleDateFormat]) : Double = {
     dateFormat match {
       case Some(dateFmt) => BasicUtils.getEpochTime(value, dateFmt).toDouble
       case None => value.toDouble
     }
   }
}