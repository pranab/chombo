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

object Covariance extends JobConfiguration with SeasonalUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "covariance"
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
	  val numAttrOrdinalsIndx = numAttrOrdinals.zipWithIndex
	   
	   
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
	  
	  var keyLen = keyFieldOrdinals match {
		     case Some(fields:Array[Integer]) => fields.length + 1
		     case None =>1
	  }
	  keyLen += (if (seasonalAnalysis) 2 else 0)
	   
	  val data = sparkCntxt.textFile(inputPath)
	  val keyedRecs = data.flatMap(line => {
		   val items = line.split(fieldDelimIn, -1)
		   val fieldStats = numAttrOrdinalsIndx.map(attr => {
		     val attrOrd = attr._1
		     val indx = attr._2
		     val key = Record(keyLen)
		     
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
		         key.addInt(cIndex.getRight())
		       }
		       case None => 
		     }	  
		     
		     //attr ordinal
		     key.addInt(attrOrd)
		     
		     //value
		     val quantVal = items(attrOrd).toDouble
		     val remaining = numAttrOrdinals.length -1 - indx
		     val value = Record(3 + remaining)
		     value.addInt(1)
		     value.addDouble(quantVal)
		     value.addDouble(quantVal * quantVal)
		     
		     //cross terms
		     for (i <- (indx+1) to (numAttrOrdinals.length -1)) {
		       val nextQuantVal = items(i).toDouble
		       value.addDouble(quantVal * nextQuantVal)
		     }
		     
		     (key,value)
		   })
		   fieldStats
	  })	
	  
	  //aggregate
	  var aggrRecs = keyedRecs.reduceByKey((v1, v2) => {
	    val aggr = Record(v1.size)
	    
	    //count
	    var i = 0
	    aggr.addInt(v1.getInt(i) + v2.getInt(i))
	    i += 1
	    
	    //sum
	    aggr.addDouble(v1.getDouble(i) + v2.getDouble(i))
	    i += 1
	    
	    //sum sq
	    aggr.addDouble(v1.getDouble(i) + v2.getDouble(i))
	    i += 1
	    
	    //cross terms
	    for (j <- i to (v1.size - 1)) {
	      aggr.addDouble(v1.getDouble(j) + v2.getDouble(j))
	    }
	    aggr
	  })
	  
	  //move quant filed ord from key to value
	  aggrRecs = aggrRecs.map(v => {
	    val key = v._1;
	    val value = v._2
	    val newKey = if (key.size > 1) {
	    	val newKey = Record(key, 0, key.size - 1)
	    	newKey
	  	} else {
	  		val newKey = Record(1)
	  		newKey.addString("all")
	  		newKey
	  	}
	    val quantFldOrd = key.getInt(key.size - 1)
	    val newValue = Record(value.size+1, value, 1)
	    newValue.addInt(0, quantFldOrd)
	    (newKey, newValue)
	  })
	  
	  
   }

}