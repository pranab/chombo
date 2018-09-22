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
import scala.util.Sorting
import org.chombo.spark.common.Record
import org.chombo.util.SeasonalAnalyzer
import org.chombo.spark.common.SeasonalUtility
import org.chombo.util.BasicUtils
import org.chombo.stats.MedianStatsManager

/**
 * Numerical fields median stats
 * @author pranab
 */
object NumericalAttrMedian extends JobConfiguration with SeasonalUtility {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
  
	   val appName = "numericalAttrMedian"
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
	  val operation = getMandatoryStringParam(appConfig, "operation.type")
	  
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

	  //median stats
	  val medStatMan = if (operation.equals("mad")) {
	    val configParams = new java.util.HashMap[String, Object]()
	    val partIdOrds = getOptionalIntListParam(appConfig, "id.fieldOrdinals");
	    val idOrdinals = partIdOrds match {
	     	case Some(idOrdinals: java.util.List[Integer]) => BasicUtils.fromListToIntArray(idOrdinals)
	     	case None => null
	    }
	    configParams.put("id.fieldOrdinals", idOrdinals)

	    val seasonalAnalysis:java.lang.Boolean = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)
	    configParams.put("seasonal.analysis", seasonalAnalysis);
	       
	    val isHdfsFile = getBooleanParamOrElse(appConfig, "hdfs.file", false)
	    configParams.put("hdfs.file", new java.lang.Boolean(isHdfsFile))
	   	    
	    val medFilePath = getMandatoryStringParam(appConfig, "med.file.path", "missing median file path")
	    configParams.put("med.filePath", medFilePath)
	    
	    val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	    configParams.put("field.delim.in", fieldDelimIn)

	    new MedianStatsManager(configParams, medFilePath, fieldDelimIn, idOrdinals, 
	        isHdfsFile, seasonalAnalysis) 
	  } else {
	    null
	  }
	   
	  val outputPrecision = getIntParamOrElse(appConfig, "output.precision", 3);
	  val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	  val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)

	  var keyLen = keyFieldOrdinals match {
		     case Some(fields:Array[Integer]) => fields.length + 1
		     case None =>1
	  }
	  val idLen = keyLen - 1
	  keyLen += (if (seasonalAnalysis) 2 else 0)

	  val data = sparkCntxt.textFile(inputPath)
	  var keyedRecs = data.flatMap(line => {
		   val items = line.split(fieldDelimIn, -1)
		   val fieldStats = numAttrOrdinals.map(attrOrd => {
		     //val attrOrd = attr
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
		     val quantVal = if (operation.equals("med")) {
		       items(attrOrd).toDouble
		     } else {
		       val med = if (keyLen == 1) {
		         medStatMan.getMedian(attrOrd)
		       } else {
		         val compKey = key.toString(0, key.size - 1)
		         medStatMan.getKeyedMedian(compKey, attrOrd)
		       }
		       val value = items(attrOrd).toDouble
		       Math.abs(value - med)
		     }
		     val value = Record(1)
		     value.addDouble(quantVal)
		     (key,value)
		   })
		   fieldStats
	  })
	  
	  //filter invalid seasonal index
	  keyedRecs = filtInvalidSeasonalIndex(keyedRecs, seasonalAnalysis, idLen)
	  
      //median or median absolute deviation
	  val statRecs = keyedRecs.groupByKey.mapValues(v => {
    	val vAr = v.toArray
    	Sorting.quickSort(vAr)
    	val size = vAr.length
    	val h = size / 2
    	var med = if (size % 2 == 1) { 
    	  vAr(h).getDouble(0) 
    	} else {
    	   (vAr(h -1).getDouble(0) + vAr(h).getDouble(0)) / 2
    	}
    	
    	if (operation.equals("mad")) med *= 1.4296
	    med
	  })
	  
	  val serStatsRecs = statRecs.map(v => {
	    v._1.toString + fieldDelimOut + BasicUtils.formatDouble(v._2, outputPrecision)
	  })  

	  if (debugOn) {
	     serStatsRecs.collect.foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     serStatsRecs.saveAsTextFile(outputPath)
	  }
	  
   }
}