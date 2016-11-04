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


package org.chombo.spark.etl

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BaseAttribute
import org.chombo.util.BasicUtils
import java.text.SimpleDateFormat

/**
 * Does simple validation checks. If you suspect the data is grossly invalid, 
 * sub sampe the data by choosing an appropriate fraction. For more rigorous validation
 * check use DataValidator
 * @author pranab
 *
 */
object SimpleDataValidator extends JobConfiguration {

   /**
 * @param args
 * @return
 */
   def main(args: Array[String]) {
	   val appName = "simpleDataValidator"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val subFieldDelimIn = appConfig.getString("sub.field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val fieldTypes = appConfig.getStringList("field.types")
	   val fieldCount = fieldTypes.size()
	   val sampleFraction = getOptionalDoubleParam(appConfig, "sample.fraction") 
	   val invalidFieldMarker = appConfig.getString("invalid.field.marker")
	   val invalidRecordMarker = appConfig.getString("invalid.record.marker")
	   val dateFromatStr = getOptionalStringParam(appConfig, "date.format")
	   val dateFormat = dateFromatStr match {
	     case Some(format:String) => Some(new SimpleDateFormat(format))
	     case None => None
	   }
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   //sample if necessary
	   val data = sparkCntxt.textFile(inputPath)
	   val sampled = sampleFraction match {
	       case Some(fraction:Double) => data.sample(false, fraction, System.currentTimeMillis().toInt)
	       case None => data
	   }
	   
	   //find invalid records
	   val processedRecords = sampled.map(line => {
	     val items = line.split(fieldDelimIn, -1)
	     val rec = if (items.length != fieldCount) {
	       invalidRecordMarker + line
	     } else {
	       var recValid = true
	       val zipped = fieldTypes.asScala.zipWithIndex
	       zipped.foreach(t => {
	         val item = items(t._2)
	         val valid = t._1 match {
	           case BaseAttribute.DATA_TYPE_INT => !item.isEmpty() && BasicUtils.isInt(item)
	           case BaseAttribute.DATA_TYPE_LONG => !item.isEmpty() && BasicUtils.isLong(item)
	           case BaseAttribute.DATA_TYPE_DOUBLE => !item.isEmpty() && BasicUtils.isDouble(item)
	           case BaseAttribute.DATA_TYPE_STRING => !item.isEmpty()
	           case BaseAttribute.DATA_TYPE_STRING_COMPOSITE => !item.isEmpty() && BasicUtils.isComposite(item, subFieldDelimIn)
	           case BaseAttribute.DATA_TYPE_DATE => {
	             //use date formatter if provided, otherwise treat as string
	             val dateValid = dateFormat match {
	               case Some(formatter:SimpleDateFormat) => !item.isEmpty() && BasicUtils.isDate(item, formatter)
	               case None => true
	             }
	             dateValid
	           }
	           case _ => throw new IllegalStateException("invalid data type specified " + t._1)
	         }
	         if (!valid) {
	           items(t._2) = invalidFieldMarker + items(t._2)
	           recValid = false
	         }
	         
	       })
	       if (recValid) {
	         line
	       } else {
	          items.mkString(fieldDelimOut)
	       }  
	     }
	     
	     rec
	   })
	   
	   //filter out invalid records
	   val invalidRecords = processedRecords.filter(r => {
	     r.contains(invalidRecordMarker) || r.contains(invalidFieldMarker)
	   })
	   val invalidRecArray = invalidRecords.collect
	   
	   if (debugOn) {
	     invalidRecArray.foreach(r => println(r))
	   }
	   
	   if(saveOutput) {	   
		   invalidRecords.saveAsTextFile(outputPath) 
   	   }
	   
   }
}