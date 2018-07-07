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
import org.chombo.util.AttributeFilter

/**
 * Splits a data set into two data sets based on various criteria e.g sub sampling, filter and 
 * presence of missing fields
 * @param args
 * @return
 */
object DataSetSplitter extends JobConfiguration {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String])  {
	   val appName = "dataSetSplitter"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val operation = getMandatoryStringParam(appConfig, "divider.operation", "missing divider operation")
	   val subSampleFraction = getConditionalMandatoryDoubleParam(operation.equals("subSample"), appConfig, 
	       "sub.sample.fraction", "missing sub sample fraction")
	   val subSampleWithReplacement = getConditionalMandatoryBooleanParam(operation.equals("subSample"), appConfig, 
	       "sub.sample.with.replacement", "missing sub sample replacement flag")
	   val filterExpr = getConditionalMandatoryStringParam(operation.equals("filter"), appConfig, 
	       "filter.expr", "missing filter expression")
	   val secondaryOutputPath = getOptionalStringParam(appConfig, "secondary.output.path")
	   val saveSecondaryOutput = secondaryOutputPath match {
	     case Some(path : String) => true
	     case None => false
	   }
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)

	   val data = sparkCntxt.textFile(inputPath)
	   if (saveSecondaryOutput) data.cache
	   val extractedData = operation match {
	     case "subSample" => {
	       data.sample(subSampleWithReplacement, subSampleFraction, 100)
	     }
	     case "filter" => {
	       val filter = new AttributeFilter(filterExpr)
	       val filtered = data.filter(r => {
	         val items = r.split(fieldDelimIn, -1)
	         filter.evaluate(items)
	       })
	       filtered
	     }
	     case "withMissingFields" => {
	       val filtered = data.filter(r => {
	         val items = r.split(fieldDelimIn, -1)
	         BasicUtils.anyEmptyField(items)
	       })
	       filtered
	     }
	   }
	   
	   if (saveSecondaryOutput && operation.equals("subSample")) extractedData.cache
	   
	   if (debugOn) {
	     val extractedDataCol = extractedData.collect
	     extractedDataCol.slice(0,20).foreach(d => println(d))
	   }	
	   if (saveOutput) {
	     extractedData.saveAsTextFile(outputPath)
	   }
	   
	   //secondary output
	   if (saveSecondaryOutput) {
		   val remainingData = operation match {
		     case "subSample" => {
		       data.subtract(extractedData)
		     }
		     case "filter" => {
		       val filter = new AttributeFilter(filterExpr)
		       val filtered = data.filter(r => {
		         val items = r.split(fieldDelimIn, -1)
		         !filter.evaluate(items)
		       })
		       filtered
		     }
		     case "withMissingFields" => {
		       val filtered = data.filter(r => {
		         val items = r.split(fieldDelimIn, -1)
		         !BasicUtils.anyEmptyField(items)
		       })
		       filtered
		     }
		     
		   }
		    
		   if (debugOn) {
			   val remainingDataCol = remainingData.collect
			   remainingDataCol.slice(0,20).foreach(d => println(d))
		   }
		   
		   if (saveOutput) {
	       	 secondaryOutputPath match {
	       	   case Some(outputPath : String) => remainingData.saveAsTextFile(outputPath)
	       	   case None => 
	       	 }
		   }
	   }
	   
   }

}