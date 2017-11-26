/*
 * chombo-spark: etl on spark
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
import org.chombo.util.BasicUtils
import org.chombo.util.SeasonalAnalyzer
import com.typesafe.config.Config
import org.chombo.spark.common.Record

object SeasonalDetector extends JobConfiguration {
   
  /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "seasonalDetector"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val attributes = getMandatoryIntListParam(appConfig, "attributes").asScala.toArray
	   val idOrdinals = getOptionalIntListParam(appConfig, "id.ordinals")
	   val timeStampFieldOrdinal = getMandatoryIntParam(appConfig, "timeStamp.field.ordinal", 
	       "missing time stamp field ordinal configuration")
	   val seasonalCycleType = getMandatoryStringParam(appConfig, "seasonal.cycle.type", 
	       "missing seasonal cycle type configuration")
	   val seasonalAnalyzer = new SeasonalAnalyzer(seasonalCycleType)
	   
	   //additional configuration
	   seasonalCycleType match {
	     case SeasonalAnalyzer.HOUR_RANGE_OF_WEEK_DAY => seasonalAnalyzer.setHourRanges(getHourGroups(appConfig))
	     case SeasonalAnalyzer.HOUR_RANGE_OF_WEEK_END_DAY => seasonalAnalyzer.setHourRanges(getHourGroups(appConfig))
	     case SeasonalAnalyzer.ANY_TIME_RANGE => seasonalAnalyzer.setTimeRanges(getTimeRange(appConfig))
	   }
	   
	   val timeZoneShiftHours = this.getIntParamOrElse(appConfig, "time.zone.shift.hours", 0)
	   if (timeZoneShiftHours > 0) seasonalAnalyzer.setTimeZoneShiftHours(timeZoneShiftHours)
       val timeStampInMili = this.getBooleanParamOrElse(appConfig, "time.stamp.in.mili", true)
	   seasonalAnalyzer.setTimeStampInMili(timeStampInMili)
	   val aggregatorType = this.getStringParamOrElse(appConfig, "aggregator.type", "average")
	   
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //key by parent cycle index, cycle index, attribute
	   val cycleKeyedRecs = data.flatMap(line => {
	     val items  =  line.split(fieldDelimIn, -1) 
	     val lineRec = Record(items)
	     val timeStamp = BasicUtils.getLongField(items, timeStampFieldOrdinal)
	     val cycleIndex = seasonalAnalyzer.getCycleIndex(timeStamp)
	     val validCycleIndex = cycleIndex >= 0
	     val cycleKeyedRecs = validCycleIndex match {
	       case true => {
	         val keyValRecs = attributes.map(a => {
	           val key = idOrdinals match {
	             case Some(idOrds : java.util.List[Integer]) => {
	               val key = Record(idOrds.size() + 3)
	               key.add(lineRec, idOrds).add(seasonalAnalyzer.getParentCycleIndex(), cycleIndex, a)
	               key
	             }
	             case None => {
	               val key = Record(3)
	               key.add(seasonalAnalyzer.getParentCycleIndex(), cycleIndex, a)
	               key
	             }
	           }
	           
	           val value = Record(1)
	           value.add(1, BasicUtils.getDoubleField(items, a))
	           (key, value)
	         })
	         keyValRecs
	       }
	       case false => {
	         Array[(org.chombo.spark.common.Record, org.chombo.spark.common.Record)]()
	       }
	     }
	   cycleKeyedRecs
	   })

	   //reduce
	   val redCycleKeyedRecs = cycleKeyedRecs.reduceByKey((v1, v2) => {
	     val count = v1.getInt(0) + v2.getInt(0)
	     val sum = v1.getDouble(1) + v2.getDouble(1)
	     val aggrRec = Record(2)
	     aggrRec.add(count, sum)
	     aggrRec
	   })
	   
	   //aggregate
	   val aggrCycleKeyedRecs = redCycleKeyedRecs.mapValues(v => {
	     val rec = Record(1)
	     aggregatorType match {
	       case "count" => rec.add(v.getInt(0))
	       case "sum" => rec.add(v.getDouble(1))
	       case "average" => rec.add(v.getDouble(1) / v.getInt(0))
	     }
	     rec
	   })
	   
       if (debugOn) {
         val records = aggrCycleKeyedRecs.collect
         records.foreach(r => println(r._1.toString() + fieldDelimOut + r._2.toString()))
       }
	   
	   if(saveOutput) {	   
	     aggrCycleKeyedRecs.saveAsTextFile(outputPath) 
	   }
	   
	   
   }
   
   
   /**
   * @param appConfig
   * @return
   */
   def getHourGroups(appConfig : Config) : java.util.Map[Integer, Integer] = {
     val hourGroupsStr = getMandatoryStringParam(appConfig, "hour.groups", "missing seasonal hout group configuration")
     BasicUtils.integerIntegerMapFromString(hourGroupsStr, BasicUtils.DEF_FIELD_DELIM, BasicUtils.DEF_SUB_FIELD_DELIM, true)
   }
   
   /**
   * @param appConfig
   * @return
   */
   def getTimeRange(appConfig : Config) :  java.util.List[org.chombo.util.Pair[Integer,Integer]]  = {
     val timeRangeStr = getMandatoryStringParam(appConfig, "time.range", "missing seasonal time range configuration")
     BasicUtils.getIntPairList(timeRangeStr, BasicUtils.DEF_FIELD_DELIM, BasicUtils.DEF_SUB_FIELD_DELIM)
   }
}