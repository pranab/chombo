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

import scala.Array.canBuildFrom

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import org.chombo.spark.common.SeasonalUtility
import org.chombo.util.BasicUtils
import org.chombo.util.SeasonalAnalyzer

import com.typesafe.config.Config

object MultiVarNumericalDistrStats extends JobConfiguration with SeasonalUtility with GeneralUtility{
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "multiVarNumericalDistrStats"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val keyFields = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val numAttrOrdinals = toIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals", "missing quant field ordinals"))
	   val seasonalTypeFldOrd = getOptionalIntParam(appConfig, "seasonal.typeFldOrd")
	   val seasonalTypeInData = seasonalTypeFldOrd match {
		     case Some(seasonalOrd:Int) => true
		     case None => false
	   }
	   val seasonalAnalysis = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)
	   val analyzerMap = creatSeasonalAnalyzerMap(this, appConfig, seasonalAnalysis, seasonalTypeInData)
	   val analyzers = creatSeasonalAnalyzerArray(this, appConfig, seasonalAnalysis, seasonalTypeInData)
	   val toCount = this.getBooleanParamOrElse(appConfig, "output.toCount", true)
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig,"save.output", true)
	   
	   val binWidths = numAttrOrdinals.map(v => {
	     val key = "bin.width." + v
	     getMandatoryDoubleParam(appConfig, key, "missing bin width")
	   })
	   val atOrdBinWidth = numAttrOrdinals.zip(binWidths)
	   val keyLen = getKeyLength(keyFields, seasonalAnalysis) + numAttrOrdinals.length

	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   val serData = if (toCount) {
	     //count
	     data.map(line => {
	       val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val key = buildKey(items, keyLen, keyFields, appConfig,analyzerMap, analyzers,seasonalAnalysis, atOrdBinWidth)
		   (key, 1)
	     }).reduceByKey((v1,v2) => v1 + v2).map(v => v._1.toString + fieldDelimOut + v._2)
	   } else {
	     //actual record
	     data.map(line => {
	       val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val key = buildKey(items, keyLen, keyFields, appConfig,analyzerMap, analyzers,seasonalAnalysis, atOrdBinWidth)
		   key.toString + fieldDelimOut + line
	     })	     
	   }
	   if (debugOn) {
         val records = serData.collect
         records.slice(0, 100).foreach(r => println(r))
       }
	   
	   if(saveOutput) {	   
	     serData.saveAsTextFile(outputPath) 
	   }	 

   }
   
   /**
   * @param items
   * @param keyLen
   * @param appConfig
   * @param analyzerMap
   * @param analyzers
   * @param seasonalAnalysis
   * @param atOrdBinWidth
   * @return
   */ 
   def buildKey(items:Array[String], keyLen:Int, keyFields: Option[Array[Int]], appConfig: Config,
       analyzerMap: (Map[String,SeasonalAnalyzer], Int, Boolean), analyzers: (Array[SeasonalAnalyzer], Int),
       seasonalAnalysis: Boolean, atOrdBinWidth: Array[(Int, Double)]) : Record =  {
	   val key = Record(keyLen)
	   addPrimarykeys(items, keyFields,  key)
	   addSeasonalKeys(this, appConfig,analyzerMap, analyzers, items, seasonalAnalysis, key)	
	   addBinKeys(items, atOrdBinWidth, key)
	   key
   }
   
   /**
   * @param sparkCntxt
   * @param config
   * @param fromList
   * @param paramName
   */
   def addBinKeys(items:Array[String], atOrdBinWidth: Array[(Int, Double)], key:Record) {
     atOrdBinWidth.foreach(v => {
       val fieldVal = items(v._1).toDouble
       val index = (fieldVal / v._2).toInt
       key.addInt(index)
     })
   }
}