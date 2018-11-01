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
 * Missing value counter row or column wise
 * @param args
 * @return
 */
object MissingValueCounter extends JobConfiguration {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String])  {
	   val appName = "missingValueCounter"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val operation = getStringParamOrElse(appConfig, "operation.dimension", "row")
	   
	   var beg = 0
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = keyFields match {
	     case Some(fields:java.util.List[Integer]) => {
	       beg = fields.size()
	       Some(fields.asScala.toArray)
	     }
	     case None => None  
	   }
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)

	   val data = sparkCntxt.textFile(inputPath)
	   var missingCounted = data.flatMap(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   if (operation.equals("row")) {
		     //row wise
		     val key =  keyFieldOrdinals match {
		       case Some(fldOrdinals: Array[Integer]) => Record(items, fldOrdinals)
		       case None => Record(line)
		     }
		     
             val count = BasicUtils.missingFieldCount(items, beg);
             val recs = ArrayBuffer[(Record, Int)]()
		     if (count > 0) {
		       val rec = (key, count)
		       recs += rec
		     }
		     recs
		   } else {
		     //column wise
		     val recs = ArrayBuffer[(Record, Int)]()
		     items.zipWithIndex.foreach(f => {
		       if (f._2 >= beg && f._1.isEmpty()) {
		         val key = Record(1)
		         key.addInt(f._2)
		         val count = 1
		         val rec = (key, count)
		         recs += rec
		       }
		     })
		     recs
		   }
	   })
	   
	   //reduce for column counters
	   missingCounted = 
	     if (operation.equals("col")) missingCounted.reduceByKey((v1, v2) => v1 + v2) 
	     else missingCounted
	   
	   
	   //serialize for output
	   val serMissingCounted = missingCounted.map(r => r._1.toString + fieldDelimOut + r._2)
	   
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