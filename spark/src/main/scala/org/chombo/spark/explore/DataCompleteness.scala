/*
 * avenir-spark: Predictive analytic based on Spark
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

/**
 * Profile based data completeness metric
 * @param args
 * @return
 */
object DataCompleteness extends JobConfiguration {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String])  {
	   val appName = "dataCompleteness"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val complProfiles = getMandatoryStringListParam(appConfig, "compl.profiles", "missing ").asScala.toList
	   val profWeights = complProfiles.map(pr => {
	     val weights = getMandatoryIntListParam(appConfig, pr + ".weight", "missing profile weight").asScala.toArray
	     (pr, weights)
	   })
	   val outputPrecision = this.getIntParamOrElse(appConfig, "output.precision", 3)
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   val data = sparkCntxt.textFile(inputPath)
	   val dataWithMetric = data.flatMap(line => {
		   val items = line.split(fieldDelimIn, -1)
		   val dataWithMetric = profWeights.map(prw => {
		     val pr = prw._1
		     val weights = prw._2
		     val fieldWeights = items.zip(weights).map(r => {
		       val exists = if (r._1.isEmpty()) 0 else 1
		       (exists, r._2.toInt)
		     })
		     val sum = fieldWeights.map(r => r._1 * r._2).reduceLeft(_ + _).toDouble
		     val metric = sum / items.length
		     pr + fieldDelimOut + line + fieldDelimOut + BasicUtils.formatDouble(metric, outputPrecision)
		   })
		   dataWithMetric
	   })	
	   
	   if (debugOn) {
	     val dataWithMetricCol = dataWithMetric.collect
	     dataWithMetricCol.slice(0,20).foreach(d => {
	       println(d)
	     })
	   }	
	   
	   if (saveOutput) {
	     dataWithMetric.saveAsTextFile(outputPath)
	   }
	   
   }

}