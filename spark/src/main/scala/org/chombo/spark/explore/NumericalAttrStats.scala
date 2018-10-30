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
import org.chombo.util.SeasonalAnalyzer
import org.chombo.spark.common.SeasonalUtility
import org.chombo.util.BasicUtils
import org.chombo.stats.NumericalAttrStatsManager

/**
 * Numerical fields tatistics
 * @author pranab
 */
object NumericalAttrStats extends JobConfiguration with SeasonalUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "numericalAttrStats"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = keyFields match {
	     case Some(fields:java.util.List[Integer]) => Some(fields.asScala.toArray)
	     case None => None  
	   }
	   
	  val numAttrOrdinals = getMandatoryIntListParam(appConfig, "attr.ordinals", 
	      "missing quant attribute ordinals").asScala.toArray
	  val condAttrOrd = getOptionalIntParam(appConfig, "cond.attr.ordinal")
	  
	   //seasonal data
	   val seasonalAnalysis = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)
	   val partBySeasonCycle = getBooleanParamOrElse(appConfig, "part.bySeasonCycle", true)
	   val seasonalAnalyzers = if (seasonalAnalysis) {
		   val seasonalCycleTypes = getMandatoryStringListParam(appConfig, "seasonal.cycleType", 
	        "missing seasonal cycle type").asScala.toArray
	        val timeZoneShiftHours = getIntParamOrElse(appConfig, "time.zoneShiftHours", 0)
	        val timeStampFieldOrdinal = getMandatoryIntParam(appConfig, "time.fieldOrdinal", 
	        "missing time stamp field ordinal")
	        val timeStampInMili = getBooleanParamOrElse(appConfig, "time.inMili", true)
	        
	        val analyzers = seasonalCycleTypes.map(sType => {
	    	val seasonalAnalyzer = createSeasonalAnalyzer(this, appConfig, sType, timeZoneShiftHours, timeStampInMili)
	        seasonalAnalyzer
	    })
	    Some((analyzers, timeStampFieldOrdinal))
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
	  val idLen = keyLen
	  keyLen += (if (seasonalAnalysis) 1 else 0)
	  keyLen += (if (seasonalAnalysis && partBySeasonCycle) 1 else 0)
	  keyLen += 2
	  
	  val data = sparkCntxt.textFile(inputPath)
	  var keyedRecs = data.flatMap(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
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
		     seasonalAnalyzers match {
		       case Some(seAnalyzers : (Array[SeasonalAnalyzer], Int)) => {
		         val timeStamp = items(seAnalyzers._2).toLong
		         val cIndex = SeasonalAnalyzer.getCycleIndex(seAnalyzers._1, timeStamp)
		         key.addString(cIndex.getLeft())
		         if (partBySeasonCycle) key.addInt(cIndex.getRight())
		       }
		       case None => 
		     }	  
		     
		     //attr ordinal
		     key.addInt(attr)
		     
		     //conditional attribute
		     condAttrOrd match {
		     	case Some(attrOrd : Int) => key.addString(items(attrOrd))
		     	case None => key.addString(NumericalAttrStatsManager.DEF_COND_ATTR_VAL)
		     }
		     
		     val quantVal = items(attr).toDouble
		     value.addDouble(quantVal)
		     value.addDouble(quantVal)
		     value.addDouble(quantVal)
		     value.addDouble(quantVal * quantVal)
		     value.addInt(1)
		     
		     (key,value)
		   })
		   fieldStats
	  })
	  
	  //filter invalid seasonal index
	  keyedRecs = filtInvalidSeasonalIndex(keyedRecs, seasonalAnalysis, idLen)
	  
	  //aggregate
	  keyedRecs = keyedRecs.reduceByKey((v1, v2) => {
	    val aggr = Record(5)
	    v1.intialize
	    v2.intialize
	    
	    //sum
	    aggr.addDouble(v1.getDouble() + v2.getDouble())
	    
	    //min
	    var d1 = v1.getDouble()
	    var d2 = v2.getDouble()
	    aggr.addDouble(if (d1 < d2) d1 else d2)
	    
	    //max
	    d1 = v1.getDouble()
	    d2 = v2.getDouble()
	    aggr.addDouble(if (d1 > d2) d1 else d2)
	    
	    //sum sq
	    aggr.addDouble(v1.getDouble() + v2.getDouble())
	    
	    //count
	    aggr.addInt(v1.getInt() + v2.getInt())
	    aggr
	  })
	  
	  //calculate stats
	  val statsRecs = keyedRecs.map(kv => {
	    val stat = Record(8)
	    kv._2.intialize
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
	     statsRecs.collect.foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     statsRecs.saveAsTextFile(outputPath)
	  }
	  
   }

}