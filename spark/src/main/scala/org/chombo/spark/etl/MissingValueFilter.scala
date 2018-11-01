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
 * Filters missing values from rows and columns
 * @param args
 * @return
 */
object MissingValueFilter extends JobConfiguration {
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
	   val maxRowMissingValues = getMandatoryIntParam(appConfig, "max.rowMissingValues", "missing row max missing values")
	   val colMissingValCounts = getMandatoryIntListParam(appConfig, "col.missingValCount", 
	       "missing column missing value count").asScala.toList
	   val maxColMissingValuesPercent = getMandatoryIntParam(appConfig, "max.colMissingValuesPercent", "missing max missing values")

	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   val data = sparkCntxt.textFile(inputPath).cache
	   val dataSize = data.count
	   
	   //columns to be excluded
	   val exColumns = scala.collection.mutable.Set[Int]()
	   for (i <- colMissingValCounts.zipWithIndex
	       if i._1 > ((dataSize * maxColMissingValuesPercent) / 100)) {
	     exColumns += i._2
	   }
	   
	   //remove rows
	   var filtData = data.filter(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   BasicUtils.missingFieldCount(items)	<= maxRowMissingValues
	   })
	   
	   //remove columns
	   filtData = if (exColumns.isEmpty) {
	     filtData
	   } else {
	     filtData.map(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val filtItems = for (it <- items.zipWithIndex
		     if !exColumns.contains(it._2)) yield it._1
		   filtItems.mkString(fieldDelimOut)
	     })
   	   }
	   
	   if (debugOn) {
	     val filtDataCol = filtData.collect
	     filtDataCol.slice(0,20).foreach(d => {
	       println(d)
	     })
	   }	
	   
	   if (saveOutput) {
	     filtData.saveAsTextFile(outputPath)
	   }
	   
   }

}