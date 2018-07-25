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
 * Profile based data completeness or validation metric
 * @param args
 * @return
 */
object DataQualityMetric extends JobConfiguration {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String])  {
	   val appName = "dataQualityMetric"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val dimension = getStringParamOrElse(appConfig, "operation.dimension", "row")
	   val complProfiles = getMandatoryStringListParam(appConfig, "compl.profiles", "missing ").asScala.toList
	   val profWeights = complProfiles.map(pr => {
	     val weights = getMandatoryDoubleListParam(appConfig, pr + ".weight", "missing profile weight").asScala.toArray
	     val weightSum = weights.reduceLeft(_ + _)
	     (pr, weights, weightSum)
	   })
	   
	   val invalidFieldMarker = getOptionalStringParam(appConfig, "invalid.field.marker")
	   val outputPrecision = this.getIntParamOrElse(appConfig, "output.precision", 3)
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   val data = sparkCntxt.textFile(inputPath)
	   val dataWithMetric = if (dimension.equals("row")) {
		   //row wise
		   val dataWithMetric = data.flatMap(line => {
			   val items = line.split(fieldDelimIn, -1)
			   val dataWithMetric = profWeights.map(prw => {
			     val prof = prw._1
			     val weights = prw._2
			     val weightSum = prw._3
			     if (items.length != weights.length) {
			       throw new IllegalStateException("weight vetor length and record length mismatch")
			     }
			     
			     //quality metric and weight list for all columns
			     val fieldWeights = items.zip(weights).map(r => {
			       val qualityFlag = invalidFieldMarker match {
			         //validation quality
			         case Some(marker:String) => if (r._1.equals(marker)) 0 else 1
			         
			         //completeness quality
			         case None => if (r._1.isEmpty()) 0 else 1
			       }
			       (qualityFlag, r._2)
			     })
			     
			     //metric for row
			     val sum = fieldWeights.map(r => r._1 * r._2).reduceLeft(_ + _).toDouble
			     val metric = sum / weightSum
			     prof + fieldDelimOut + BasicUtils.formatDouble(metric, outputPrecision) + fieldDelimOut  + line 
			   })
			   dataWithMetric
		   })	
		   dataWithMetric
	   } else {
		   //column wise
		   data.cache
		   val rowCount = data.count
		   val dataWithMetric = data.flatMap(line => {
			   val items = line.split(fieldDelimIn, -1)
			   	 val dataWithMetric = profWeights.map(prw => {
			   		 val prof = prw._1
			   		 val weights = prw._2
			   		 if (items.length != weights.length) {
			   			 throw new IllegalStateException("weight vetor length and record length mismatch")
			   		 }
			   		 //all columns
			   		 val colWt = weights.zipWithIndex.map(r => {
			   			 val colIndex = r._2
			   			 val key =  (prof, colIndex)
			   			 val qualityFlag = invalidFieldMarker match {
			   				 //validation quality
			   			 	case Some(marker:String) => if (items(colIndex).equals(marker)) 0 else 1
			         
			   			 	//completeness quality
			   			 	case None => if (items(colIndex).isEmpty()) 0 else 1
			   			 }
			   			 val wt = r._1 * qualityFlag
			   			(key, wt)
			   		 })
			   		colWt 
			   })
			   
			   //profile , column index and weight
			   val metricArray = ArrayBuffer[((String, Int),Double)]()
			   dataWithMetric.foreach(a =>  metricArray ++= a)
			   metricArray
		   })
		   val colWithMetric = dataWithMetric.reduceByKey(_ + _).map(r => {
		     val prof = r._1._1
		     val col = r._1._2
		     val wt = r._2 / rowCount
		     prof + fieldDelimOut + col + fieldDelimOut + BasicUtils.formatDouble(wt, outputPrecision)
		   })
	     colWithMetric
	   }
	   
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