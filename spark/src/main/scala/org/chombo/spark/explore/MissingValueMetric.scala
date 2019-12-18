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

/**
 * Missing value counter row or column wise
 * @param args
 * @return
 */
object MissingValueMetric extends JobConfiguration with GeneralUtility {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String])  {
	   val appName = "missingValueMetric"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val operation = getStringParamOrElse(appConfig, "operation.dimension", "row")
	   val fieldWeights = getMandatoryIntDoubleMapParam(appConfig, "field.weights", "missing field weight list").asScala
	   val numFields = fieldWeights.size
	   var sumWeight = 0.0
	   for ((k,v) <- fieldWeights) {
	     sumWeight += v
	   }
	   val tagMetric = getBooleanParamOrElse(appConfig, "tag.metric", false)
	   val missingValueTag = getOptionalStringParam(appConfig, "missing.tag")
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val dataSetDistrFilePath = getOptionalStringParam(appConfig, "distr.FilePath")
	   val distrBinWidth = getDoubleParamOrElse(appConfig, "distr.binWidth", 0.05)
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)

	   val data = sparkCntxt.textFile(inputPath).cache
	   val totCount = data.count
	   
	   var missingCounted = data.flatMap(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   if (operation.equals("row")) {
		     //row wise
		     val key =  Record("all")
		     
         var complCount = 0
         var sum = 0.0
         val count = missingValueTag match {
           case Some(tag) => {
             for ((k,v) <- fieldWeights) {
               val isNull = BasicUtils.isNull(items(k), tag)
               if (!isNull)  {
                 complCount += 1
                 sum += v
               }
             }
             sum /= sumWeight
             (complCount, sum)}
           case None => {
             for ((k,v) <- fieldWeights) {
               val isMissing = items(k).isEmpty()
               if (!isMissing) {
                 complCount += 1
                 sum += v
               }
             }
             sum /= sumWeight
             (complCount, sum)}
         }
         val recs = ArrayBuffer[(Record, Record)]()
		     val valrec = if (tagMetric) Record(4) else  Record(3)
		     valrec.addString(line)
		     if (tagMetric) {
		       valrec.addString("cmp")
		     }
		     valrec.addInt(count._1)
		     valrec.addDouble(count._2)
		     val rec = (key, valrec)
		     recs += rec
		     recs
		   } else {
		     //column wise
		     val recs = ArrayBuffer[(Record, Record)]()
		     fieldWeights.foreach(r => {
		       val fld = r._1
		       val wt = r._2
		       val isMissing = missingValueTag match {
		         case Some(tag) => BasicUtils.isNull(items(fld), tag)
		         case None => items(fld).isEmpty()
		       }
		       if (!isMissing) {
  		       val key = Record(1)
  		       key.addInt(fld)
  		       val valrec = new Record(1)
  		       valrec.addInt(1)
  		       val rec = (key, valrec)
  		       recs += rec
		       }
		     })
		     recs
		   }
	   })
	   
	   //reduce for column counters
	   missingCounted = 
	     if (operation.equals("col")){ 
	       missingCounted.reduceByKey((v1, v2) =>{ 
	       val valrec = new Record(1)
		     valrec.addInt(v1.getInt(0) + v2.getInt(0))
		     valrec
	      }).mapValues(v => {
	        val count = v.getInt(0)
	        val valrec = new Record(2)
	        valrec.addInt(count)
	        valrec.addDouble(count.toDouble/totCount)
	        valrec
	      })} else {
	       missingCounted
	     }
	   
	   //data set completeness metric distribution
	   dataSetDistrFilePath match {
	     case Some(path) => {
	       missingCounted.cache
	       val metrics = missingCounted.map(r => r._2)
	       val index = if (tagMetric) 3 else 2
	       val hist = getColumnDistrStat(metrics, index, distrBinWidth)
	       reflect.io.File(path).writeAll(hist.withExtendedOutput(true).toString())
	     }
	     case None =>
	   }
	   
	   //serialize for output
	   val serMissingCounted = missingCounted.map(r => {
	     if (operation.equals("col"))
	       r._1.toString + fieldDelimOut + r._2.withFloatPrecision(precision).toString
	     else 
	       r._2.withFloatPrecision(precision).toString
	   })
	   
     if (debugOn) {
       var records = serMissingCounted.collect
       records = 
         if (operation.equals("row")) records.slice(0,20) 
         else records
       records.foreach(r => println(r))
     }
	   
	   if(saveOutput) {	   
	     serMissingCounted.saveAsTextFile(outputPath) 
	   }
	   
   }
}