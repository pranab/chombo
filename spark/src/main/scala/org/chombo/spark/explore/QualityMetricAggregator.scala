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
import org.chombo.util.BasicUtils
import scala.collection.mutable.ArrayBuffer
import org.chombo.spark.common.GeneralUtility
import org.apache.spark.util.LongAccumulator


/**
 * Aggregation of different data quality metrics
 * @param args
 * @return
 */

object QualityMetricAggregator extends JobConfiguration with GeneralUtility {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String])  {
	   val appName = "qualityMetricAggregator"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val recLen = getMandatoryIntParam(config, "rec.len", "missing record length")
	   val metricWeight = getMandatoryStringDoubleMapParam(config, "metric.weight", "missing metric weight")
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   val data = sparkCntxt.textFile(inputPath)
	   val outData = data.map(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val rec = items.slice(0, recLen).mkString(fieldDelimIn)
		   val qualityTag = items(recLen + 1)
		   val metricVal = items(recLen + 3).toDouble
		   val valRec = Record(2)
		   valRec.addDouble(metricWeight.get(qualityTag))
		   valRec.addDouble(metricVal)
		   (rec, valRec)
	   }).groupByKey.map(r => {
	     val values = r._2.toArray
	     val weightedMetric = getColumnWeightedAverage(values, 0, 1)
	     r._1 + fieldDelimOut + BasicUtils.formatDouble(weightedMetric, precision)
	   })
	   
     if (debugOn) {
       outData.collect.slice(0,50) .foreach(r => println(r))
     }
	   
	   if(saveOutput) {	   
	     outData.saveAsTextFile(outputPath) 
	   }
	   
   }
  
}