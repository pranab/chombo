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
import org.chombo.stats.HistogramStat
import org.chombo.stats.HistogramUtility
import java.io.FileInputStream
import scala.collection.mutable.Map
import org.chombo.spark.common.SeasonalUtility
import org.chombo.util.SeasonalAnalyzer

object NumericalAttrDistrStats extends JobConfiguration with SeasonalUtility {
  
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
	   var keyLen = keyFieldOrdinals match {
		     case Some(fields:Array[Integer]) => fields.length + 1
		     case None =>1
	   }
	   val idLen = keyLen - 1
	   
	   //val keyFieldOrdinals = getMandatoryIntListParam(appConfig, "id.field.ordinals").asScala.toArray
	   val numAttrOrdinals = getMandatoryIntListParam(appConfig, "num.attr.ordinals", "").asScala.toArray
	   
	   //field ordinal and bin width
	   val binWidths = numAttrOrdinals.map(ord => {
	     //attribute bin width tuple
	     val key = "attrBinWidth." + ord
	     (ord, getMandatoryIntParam(appConfig, key, "missing bin width"))
	   })
	   
	   val extendedOutput = getBooleanParamOrElse(appConfig, "extended.output", true)
	   val outputPrecision = getIntParamOrElse(appConfig, "output.precision", 3);
	   val refDistrFilePath = getOptionalStringParam(appConfig, "reference.distr.file.path")
	   
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
	   keyLen += (if (seasonalAnalysis) 2 else 0)
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //key with record key and attr ordinal and value map of counts
	   var keyedRecs = data.flatMap(line => {
		   val items = line.split(fieldDelimIn, -1)
		   val attrValCount = binWidths.map(ord => {
		     //key
			 val attrKeyRec = keyFieldOrdinals match {
			     //with partition key and field ordinal
			     case Some(fields:Array[Integer]) => {
			       val rec = Record(keyLen, items, fields)
			       
			       //seasonality cycle
		           seasonalAnalyzers match {
		             case Some(seAnalyzers : (Array[SeasonalAnalyzer], Int)) => {
		            	 val timeStamp = items(seAnalyzers._2).toLong
		            	 val cIndex = SeasonalAnalyzer.getCycleIndex(seAnalyzers._1, timeStamp)
		            	 rec.addString(cIndex.getLeft())
		            	 rec.addInt(cIndex.getRight())
		             }
		             case None => 
			       }	  
			       
			       rec.addInt(ord._1.toInt)
			       rec
			     }
			     //filed ordinal only
			     case None => Record(1, ord._1.toInt)
			 }
		     
			 //value is histogram
		     val attrValRec =new HistogramStat(ord._2)
		     attrValRec.
		     	withExtendedOutput(extendedOutput).
		     	withOutputPrecision(outputPrecision)
		     val attrVal = items(ord._1).toDouble
		     if (debugOn) {
		       //println("attrVal: " + attrVal)
		     }
		     attrValRec.add(attrVal)
		     (attrKeyRec, attrValRec)
		   })
		   
		   attrValCount
	   })
	   
	  //filter invalid seasonal index
	  keyedRecs  = 
	  if (seasonalAnalysis) {
	    val filt = keyedRecs.filter(v => {
	      val key = v._1
	      val ci = idLen + 1
	      key.getInt(ci) >= 0
	    })
	    filt
	  } else {
	    keyedRecs
	  }
	   
	   //merge histograms
	   val stats = keyedRecs.reduceByKey((h1, h2) => h1.merge(h2))
	   val colStats = stats.collect
	   
	   //reference stats
	   val refStats = refDistrFilePath match {
	     case Some(path:String) => {
	       val refStats = HistogramUtility.createHiostograms(new FileInputStream(path), keyLen, true)
	       val stats = refStats.asScala.map(kv => {
	         //last element of key is field ordinal
	         val rec = Record(kv._1)
	         rec.addInt(keyLen-1, kv._1(keyLen-1).toInt)
	         (rec, kv._2)
	       })
	       Some(stats)
	     }
	     case None => None
	   }

	   //append KL divergence
	   val modStats = colStats.map(v => {
	     val stat = refStats match {
	       case Some(stats:Map[Record,HistogramStat]) => {
	         val key = v._1
	         val refDistr = stats.get(key).get
	         val thisDistr = v._2
	         val diverge = HistogramUtility.findKullbackLeiblerDivergence(refDistr, thisDistr)
	         (v._1, v._2, diverge.getLeft().toDouble, diverge.getRight().toInt)
	       }
	       case None => (v._1, v._2, 0.0, 0)
	     }
	     stat
	   })
	   
	   if (debugOn) {
	     modStats.foreach(s => {
	       println("id:" + s._1)
	       println("distr:" + s._2)
	       println("dvergence:" + s._3 + " " + s._4)
	     })
	   }
	   
	   if (saveOutput) {
	     val stats = sparkCntxt.parallelize(modStats)
	     stats.saveAsTextFile(outputPath)
	   }
	   
   }}