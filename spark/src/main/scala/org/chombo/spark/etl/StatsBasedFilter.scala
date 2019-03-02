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

package org.chombo.spark.etl

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import scala.collection.mutable.ArrayBuffer
import org.chombo.spark.common.GeneralUtility
import org.chombo.stats.NumericalAttrStatsManager
import org.chombo.stats.UniqueValueCounterStatsManager
import org.chombo.spark.common.SeasonalUtility
import org.chombo.util.SeasonalAnalyzer


/**
 * Statistics based filter
 * @author pranab
 */
object StatsBasedFilter extends JobConfiguration with GeneralUtility with SeasonalUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "statsBasedFilter"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFields = getMandatoryIntListParam(appConfig, "id.fieldOrdinals", "missing id field ordinals")
	   val keyFieldOrdinals = toIntArray(keyFields)
	   val idOrdinals = BasicUtils.fromListToIntArray(keyFields)
	   val numAttrOrdinals = toIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals", "missing quant field ordinals"))
	   val seasonalAnalysis = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)
	   val statsFilePath = getMandatoryStringParam(appConfig, "stats.filePath", "missing stat file path")
	   
	   //data with these conditions will be filtered out
	   val filterTypes = getMandatoryStringListParam(appConfig, "filter.types", "missing filter type").asScala.toArray
	   val allFilterTypes = Array[String]("countBelow", "countAbove", "meanBelow", "meanAbove", "stdDevBelow", "stdDevAbove", 
	       "rangeBelow", "rangeAbove", "cardinalityBelow", "cardinalityAbove")
	   assertMultipleStringMember(filterTypes, allFilterTypes, "invalid filter type")
	   var thresholds = Map[String,Double]()
	   filterTypes.foreach(fi => {
	     val key = fi +".threshold"
	     val threshold = getMandatoryDoubleParam(appConfig, key, "missing filter threshold")
	     thresholds += {fi -> threshold}
	   })
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   //keys to be discarded
	   val statsManager =  new NumericalAttrStatsManager(statsFilePath, ",",idOrdinals, seasonalAnalysis, false);
	   val cardFilter = filterTypes.find(s => s.equals("cardinalityBelow") || s.equals("cardinalityAbove"))
	   val uniqueCountManager = cardFilter match {
	     case Some(filt) => {
	       val uniqueCountFilePath = getMandatoryStringParam(appConfig, "uniqueCount.filePath", "missing unique count file path")
	       val uniqueCountManager = new UniqueValueCounterStatsManager(uniqueCountFilePath, ",",idOrdinals, seasonalAnalysis, false); 
	       Some(uniqueCountManager)
	     }
	     case None => None
	   }
	   val retainedKeys = getRetainedKeys(statsManager, uniqueCountManager, filterTypes, numAttrOrdinals, thresholds, fieldDelimOut)
	   if (debugOn) {
	     println("keys to be retained")
	     retainedKeys.foreach(k => println(k))
	   }
	   
	   //seasonality
	   val seasonalTypeFldOrd = getOptionalIntParam(appConfig, "seasonal.typeFldOrd")
	   val (analyzersMap, timeStampFieldOrdinal, timeStampInMili) = creatSeasonalAnalyzerMap(this, appConfig, seasonalAnalysis)
	   val analyzers = analyzersMap.values.toArray
	   
	   //key length
	   val keyLen =  if(seasonalAnalysis)  keyFieldOrdinals.size + 2 + 1 else keyFieldOrdinals.size + 1
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   var filtData = data.filter(line => {
	     val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		 val key = Record(keyLen - 1)
		 populateFields(fields, keyFieldOrdinals, key)
		 if (seasonalAnalysis) {
		     //add seasonal realted fields to key
			 findSeasonalIndex(seasonalTypeFldOrd, fields, analyzersMap, analyzers, timeStampFieldOrdinal, key)
		 }
	     
	     //to be retained only if all quant fields meet criteria
	     var toBeRetained = true
	     numAttrOrdinals.foreach(a => {
		     val keyWithAttr = Record(keyLen, key)
		     keyWithAttr.addInt(a)
		     val keyStr = keyWithAttr.toString()
		     toBeRetained &&= retainedKeys.contains(keyStr)
	     })
	    toBeRetained
	   })
	   
	  filtData = filtData.map(line => {
	     val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	     fields.mkString(fieldDelimOut)
	  }) 
	  
	  if (debugOn) {
	     filtData.collect.slice(0,100).foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     filtData.saveAsTextFile(outputPath)
	  }
   }
   
   /**
   * @param seasonalTypeFldOrd
   * @param fields
   * @param analyzersMap
   * @param analyzers
   * @param timeStampFieldOrdinal
   * @param key
   * @return
   */
   def findSeasonalIndex(seasonalTypeFldOrd:Option[Int], fields:Array[String], analyzersMap: Map[String, SeasonalAnalyzer],
       analyzers: Array[SeasonalAnalyzer], timeStampFieldOrdinal:Int, key:Record) {
	   seasonalTypeFldOrd match {
	     //seasonal type field in data
	     case Some(seasonalOrd:Int) => {
	       val seasonalType = fields(seasonalOrd)
	       val analyzer = analyzersMap.get(seasonalType).get
	       val timeStamp = fields(timeStampFieldOrdinal).toLong
	       val cIndex = SeasonalAnalyzer.getCycleIndex(analyzer, timeStamp)
	       key.addString(cIndex.getLeft())
	       key.addInt(cIndex.getRight())
	     }
	     
	     //seasonal type in configuration
	     case None => {
	       val timeStamp = fields(timeStampFieldOrdinal).toLong
	       val cIndex = SeasonalAnalyzer.getCycleIndex(analyzers, timeStamp)
	       key.addString(cIndex.getLeft())
	       key.addInt(cIndex.getRight())
	     }
	   }
   }
   
   /**
   * @param statsManager
   * @param filterTypes
   * @param numAttrOrdinals
   * @param thresholds
   * @param fieldDelimOut
   * @return
   */
   def getRetainedKeys(statsManager:NumericalAttrStatsManager, uniqueCountManager:Option[UniqueValueCounterStatsManager],
       filterTypes:Array[String], numAttrOrdinals:Array[Int], 
       thresholds:Map[String,Double], fieldDelimOut:String) : Set[String] = {
	   //keys to be discarded
	   val retainedKeys = scala.collection.mutable.Set[String]()
	   val allKeys = statsManager.getAllKeys().asScala
	   
	   //keys without attr ordinal
	   allKeys.foreach(k => {
	     
	     //each attribute 
	     numAttrOrdinals.foreach(a => {
	    	 var retain = true
	    	 
	    	 //each filter type
		     filterTypes.foreach(f => {
		       val threshold = thresholds.get(f).get
		       f match {
		         case "countBelow" => {
		           val count = statsManager.getCount(k, a)
		           retain &&= (count < threshold.toInt)
		         }
		         case "countAbove" => {
		           val count = statsManager.getCount(k, a)
		           retain &&= (count >= threshold.toInt)
		         }
		         case "meanBelow" => {
		           val mean = statsManager.getMean(k, a)
		           retain &&= (mean < threshold)
		         }
		         case "meanAbove" => {
		           val mean = statsManager.getMean(k, a)
		           retain &&= (mean >= threshold)
		         }
		         case "stdDevBelow" => {
		           val stdDev = statsManager.getStdDev(k, a)
		           retain &&= (stdDev < threshold)
		         }
		         case "stdDevAbove" => {
		           val stdDev = statsManager.getStdDev(k, a)
		           retain &&= (stdDev >= threshold)
		         }
		         case "rangeBelow" => {
		           val range = statsManager.getMax(k, a) - statsManager.getMin(k, a)
		           retain &&= (range < threshold)
		         }
		         case "rangeAbove" => {
		           val range = statsManager.getMax(k, a) - statsManager.getMin(k, a)
		           retain &&= (range >= threshold)
		         }
		         
		         case "cardinalityBelow" => {
		           uniqueCountManager match {
		             case Some(countManager) => {
		               val count = countManager.getCount(k, a)
		               retain &&= (count < threshold.toInt)
		             }
		             case None =>
		           }
		         }
		         case "cardinalityAbove" => {
		           uniqueCountManager match {
		             case Some(countManager) => {
		               val count = countManager.getCount(k, a)
		               retain &&= (count >= threshold.toInt)
		             }
		             case None =>
		           }
		         }

		         
		       }
		     })
		     if (retain) {
		       //add attr ordinal to key
		       val key = k + fieldDelimOut + a
		       retainedKeys += key
		     }
	     })
	   })
     
	  retainedKeys.toSet
   }

}