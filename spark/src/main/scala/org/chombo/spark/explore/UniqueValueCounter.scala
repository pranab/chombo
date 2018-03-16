/*
 * chombo-spark: etl on spark
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

object UniqueValueCounter extends JobConfiguration {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "categoricalAttrDistrStats"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val catFieldOrdinals = getMandatoryIntListParam(appConfig, "cat.field.ordinals").asScala.toArray
	   val uniqueValCount = getBooleanParamOrElse(appConfig, "count.values", false)
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)

	   val data = sparkCntxt.textFile(inputPath)
	   
	   //values for each column
	   val colValues = data.flatMap(line => {
		   val items = line.split(fieldDelimIn, -1)
		   val values = catFieldOrdinals.map(i => {
		     val colIndex = i.toInt
		     val valSet = Set[String](items(colIndex))
		     (colIndex, valSet)
		   })
		   values
	   })
	   
	   //reduce
	   val colUniqueValues = colValues.reduceByKey((v1, v2) => v1 ++ v2)
	   
	   if (uniqueValCount) {
		   //unique values count
		   val colUniqueValuesCount = colUniqueValues.mapValues(v => v.size)

	      if (debugOn) {
		     val uniqueValuesCount = colUniqueValuesCount.collect
		     uniqueValuesCount.foreach(line => println(line))
		   }
		   
		   if (saveOutput) {
		     colUniqueValuesCount.saveAsTextFile(outputPath)
		   }
	     
	   } else {
		   //actual unique values
		   if (debugOn) {
		     val uniqueValues = colUniqueValues.collect
		     uniqueValues.foreach(line => println(line))
		   }
		   
		   if (saveOutput) {
		     colUniqueValues.saveAsTextFile(outputPath)
		   }
	   }
   }
}