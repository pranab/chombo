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

package org.chombo.spark.etl

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import scala.collection.mutable.ArrayBuffer

/**
 * Data quality metric based filter
 * @param args
 * @return
 */
object QualityMetricBasedFilter extends JobConfiguration {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String])  {
	   val appName = "missingValueFilter"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val metricThreshold = getMandatoryStringDoubleMapParam(appConfig, "metric.threshold", "missing thtesholds").asScala
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   val data = sparkCntxt.textFile(inputPath).cache
	   for (th <- metricThreshold) {
		   val prof = th._1
		   val threshold = th._2
		   val filtData = data.filter(line => {
			   val items = line.split(fieldDelimIn, -1)
			   items(0).equals(prof) && items(1).toDouble > threshold
		   }).map(line => {
		     val pos = BasicUtils.findOccurencePosition(line, fieldDelimIn, 2, true) + fieldDelimIn.length()
		     line.substring(pos)
		   })
		   
		   if (debugOn) {
		     val filtDataCol = filtData.collect
		     filtDataCol.slice(0,20).foreach(d => {
		       println(d)
		     })
		   }	
		   
		   if (saveOutput) {
		     filtData.saveAsTextFile(outputPath + "/" + prof)
		   }
	   }
   }
}