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
import org.chombo.util.SeasonalAnalyzer;
import org.chombo.util.BasicUtils

object NumericalAttrStats extends JobConfiguration {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "numericalAttrDistrStats"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFields = getOptionalIntListParam(appConfig, "id.field.ordinals")
	   val keyFieldOrdinals = keyFields match {
	     case Some(fields:java.util.List[Integer]) => Some(fields.asScala.toArray)
	     case None => None  
	   }
	   
	  val numAttrOrdinals = getMandatoryIntListParam(appConfig, "attr.ordinals", 
	      "missing quant attribute ordinals").asScala.toArray
	  val seasonalAnalysis = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)
	  val seasonalAnalyzer = if (seasonalAnalysis) {
	    val seasonalCycleType = getMandatoryStringParam(appConfig, "seasonal.cycleType", "missing seasonal cycle type")
	    val seasonalAnalyzer = new SeasonalAnalyzer(seasonalCycleType)
	    if (SeasonalAnalyzer.isHourRange(seasonalCycleType)) {
	      val hourRanges = BasicUtils.integerIntegerMapFromString("seasonal.hourGroups", BasicUtils.DEF_FIELD_DELIM, 
	          BasicUtils.DEF_SUB_FIELD_DELIM, true)
	      seasonalAnalyzer.setHourRanges(hourRanges)
	    }
	    val timeZoneShiftHours = getIntParamOrElse(appConfig, "time.zoneShiftHours", 0)
	    if (timeZoneShiftHours > 0) {
	      seasonalAnalyzer.setTimeZoneShiftHours(timeZoneShiftHours)
	    }
	    val timeStampFieldOrdinal = getMandatoryIntParam(appConfig, "time.fieldOrdinal", 
	        "missing time stamp field ordinal")
	    val timeStampInMili = getBooleanParamOrElse(appConfig, "time.inMili", true)
	    seasonalAnalyzer.setTimeStampInMili(timeStampInMili)
	    Some((seasonalAnalyzer, timeStampFieldOrdinal))
	  } else {
	    None
	  }
	  
	  val outputPrecision = getIntParamOrElse(appConfig, "output.precision", 3);
	  val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	  val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	  
	  //key length
	  var keyLen = 0
	  keyFieldOrdinals match {
	    case Some(fields : Array[Integer]) => keyLen +=  fields.length
	    case None =>
	  }
	  keyLen += (if (seasonalAnalysis) 1 else 0)
	  keyLen += 1
	  
	  val data = sparkCntxt.textFile(inputPath)
	  val keyedData = data.flatMap(line => {
		   val items = line.split(fieldDelimIn, -1)
		   val fieldStats = numAttrOrdinals.map(attr => {
		     val key = Record(keyLen)
		     val value = Record(5)
		     
		     //partioning fields
		     keyFieldOrdinals match {
	           case Some(fields : Array[Integer]) => {
	             for (kf <- fields) {
	               key.addString(items(kf))
	             }
	           }
	           case None =>
	         }
		     
		     //seasonality cycle
		     seasonalAnalyzer match {
		       case Some(seAnalyzer : (SeasonalAnalyzer, Int)) => {
		         val timeStamp = items(seAnalyzer._2).toLong
		         val cycleIndex = seAnalyzer._1.getCycleIndex(timeStamp)
		         key.addInt(cycleIndex)
		       }
		       case None => 
		     }	  
		     
		     //attr ordinal
		     key.addInt(attr)

		     val quantVal = items(attr).toDouble
		     value.addDouble(quantVal)
		     value.addDouble(quantVal)
		     value.addDouble(quantVal)
		     value.addDouble(quantVal * quantVal)
		     value.addInt(1)
		     
		     (key,value)
		   })
		   fieldStats
	  }).reduceByKey((v1, v2) => {
	    val aggr = Record(5)
	    aggr.addDouble(v1.getDouble() + v2.getDouble())
	    val d1 = v1.getDouble()
	    val d2 = v2.getDouble()
	    aggr.addDouble(if (d1 < d2) d1 else d2)
	    aggr.addDouble(if (d1 > d2) d1 else d2)
	    aggr.addDouble(v1.getDouble() + v2.getDouble())
	    aggr.addInt(v1.getInt() + v2.getInt())
	    aggr
	  })
	  
	  //calculate stats
	  val stats = keyedData.map(kv => {
	    val stat = Record(8)
	    val sum = kv._2.getDouble()
	    val min = kv._2.getDouble()
	    val max = kv._2.getDouble()
	    val sumSq = kv._2.getDouble()
	    val count = kv._2.getInt()
	    val mean = sum / count
	    val variance = sumSq / count - mean * mean
	    val stdDev = Math.sqrt(variance)
	    
	    stat.addDouble(sum)
	    stat.addDouble(sumSq)
	    stat.addInt(count)
	    stat.addDouble(mean)
	    stat.addDouble(variance)
	    stat.addDouble(stdDev)
	    stat.addDouble(min)
	    stat.addDouble(max)
	    
	    Record.floatPrecision = outputPrecision
	    kv._1.toString() + fieldDelimOut + stat.toString
	  })
	  
	  if (debugOn) {
	     stats.foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     stats.saveAsTextFile(outputPath)
	  }
	  
   }

}