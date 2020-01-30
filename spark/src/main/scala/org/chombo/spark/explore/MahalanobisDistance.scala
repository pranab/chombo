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
import scala.collection.mutable.ArrayBuffer
import org.chombo.spark.common.Record
import org.chombo.util.SeasonalAnalyzer
import org.chombo.spark.common.SeasonalUtility
import org.chombo.util.BasicUtils
import org.chombo.stats.NumericalAttrStatsManager
import org.chombo.spark.common.GeneralUtility

/**
 * Finds Mahalonobis distance
 * @param args
 * @return
 */
object MahalanobisDistance extends JobConfiguration with SeasonalUtility with GeneralUtility {
  
    /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "mahalanobisDistance"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFieldOrdinals = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val numAttrOrdinals = getMandatoryIntListParam(appConfig, "attr.ordinals", 
	      "missing quant attribute ordinals").asScala.toArray
	   val numAttrOrdinalsIndx = numAttrOrdinals.zipWithIndex
	   val dimension = numAttrOrdinals.size
	   
	   
	   //seasonal data
	   val seasonalAnalysis = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)
	   val seasonalAnalyzers =  creatOptionalSeasonalAnalyzerArray(this, appConfig, seasonalAnalysis)
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, seasonalAnalysis, 1)
	   
	   val outputPrecision = getIntParamOrElse(appConfig, "output.precision", 3);
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   //TBD
	   
	   
   } 
  
}