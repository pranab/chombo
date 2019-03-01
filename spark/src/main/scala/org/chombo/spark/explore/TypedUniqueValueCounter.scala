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

package org.chombo.spark.explore

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.spark.common.Record
import org.chombo.spark.common.GeneralUtility
import org.chombo.util.BasicUtils

/**
 * Counts unique values as typed data
 * @author pranab
 */

object TypedUniqueValueCounter extends JobConfiguration with GeneralUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "autoCorrelation"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val attrOrdinals = toIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	   var attrTypes = Map[Int, String]()
	   attrOrdinals.foreach(a => {
	     val key = a + ".type"
	     val aType = getMandatoryStringParam(appConfig, key, "missing attribute type")
	     attrTypes += (a -> aType)
	   })
	   val keyFields = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val keyLen = getOptinalArrayLength(keyFields, 2)
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //unique value occurrence counts
	   val uniqValueOccCounts = data.flatMap(line => {
		   val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val valueCounts = attrOrdinals.map(a => {
			   val key = Record(keyLen)
			   keyFields match {
			     case Some(kFields) => populateFields(fields, kFields, key) 
			     case None =>  
			   }
			   
			   //filed index and typed value
			   key.addInt(a)
			   val aType = getMapValue(attrTypes, a, "missing data type for attribute at " + a)
			   aType match {
			     case "string" => key.addString(fields(a))
			     case "int" => key.addInt(fields(a).toInt)
			     case "double" => key.addDouble(fields(a).toDouble)
			     case _ => BasicUtils.assertFail("unsupported data type " + aType)
			   }
			   (key, 1)
		   })
	       valueCounts
	   }).reduceByKey((v1, v2) => v1 + v2)
	   
	   //unique value count
	   val uniqValueCounts = uniqValueOccCounts.map(r => {
	     val key = r._1
	     val newKey = Record(key.size - 1, key)
	     (newKey, 1)
	   }).reduceByKey((v1, v2) => v1 + v2).map(r => r._1.toString() + fieldDelimOut + r._2)
	   
	  if (debugOn) {
	     uniqValueCounts.collect.foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     uniqValueCounts.saveAsTextFile(outputPath)
	  }
	   
	   
   }

}