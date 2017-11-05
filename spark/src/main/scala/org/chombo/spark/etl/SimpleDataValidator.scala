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
	   val fieldTypes = getMandatoryStringListParam(appConfig, "field.types", "missing field type list")
	   val fieldCount = fieldTypes.size()
	   val sampleFraction = getOptionalDoubleParam(appConfig, "sample.fraction") 
	   val invalidFieldMarker = getStringParamOrElse(appConfig, "invalid.field.marker", "[X]")
	   val invalidRecordMarker = getStringParamOrElse(appConfig, "invalid.record.marker", "[XX]")
	   val dateFromatStr = getOptionalStringParam(appConfig, "date.format")
	   val dateFormat = dateFromatStr match {
	     case Some(format:String) => Some(new SimpleDateFormat(format))
	     case None => None
	   }
	   val checkMissingFieldOnly = getBooleanParamOrElse(appConfig, "check.missing.field.only", false)
	   val outputValidRecs = getBooleanParamOrElse(appConfig, "output.valid.recs", false)
	   val invalidRecsFilePath = getOptionalStringParam(appConfig, "invalid.recs.file.path")
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   //accumulators
	   val invalidRecCount = sparkCntxt.accumulator[Long](0, "invalidRecCount")

	   
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
	         val item = items(t._2).trim()
	         val status = !item.isEmpty()
	         val valid = t._1 match {
	           case BaseAttribute.DATA_TYPE_INT => {
	             if (!checkMissingFieldOnly && status)
	               BasicUtils.isInt(item)
	             else 
	               status
	           }
	           case BaseAttribute.DATA_TYPE_LONG => {
	             if (!checkMissingFieldOnly && status)
	               BasicUtils.isLong(item)
	             else 
	               status
	           }
	           case BaseAttribute.DATA_TYPE_DOUBLE => {
	             if (!checkMissingFieldOnly && status)
	               BasicUtils.isDouble(item)
	             else 
	               status
	           }
	           case BaseAttribute.DATA_TYPE_STRING => status
	           case BaseAttribute.DATA_TYPE_STRING_COMPOSITE => {
	             if (!checkMissingFieldOnly && status)
	               BasicUtils.isComposite(item, subFieldDelimIn)
	             else 
	               status
	           }
	           case BaseAttribute.DATA_TYPE_DATE => {
	             //use date formatter if provided, otherwise treat as string
	             val dateValid = dateFormat match {
	               case Some(formatter:SimpleDateFormat) => {
	                 if (!checkMissingFieldOnly && status)
	                   BasicUtils.isDate(item, formatter)
	                 else 
	                   status
	               }
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
	   
	   processedRecords.cache
	   
	   //filter out valid or invalid records
	   val filtRecords = processedRecords.filter(r => {
	     val status = outputValidRecs match {
	       case true => {
	         val valid = !r.contains(invalidRecordMarker) && !r.contains(invalidFieldMarker)
	         if (!valid) invalidRecCount += 1
	         valid
	       }
	       case false => r.contains(invalidRecordMarker) || r.contains(invalidFieldMarker)
	     }
	     status
	   })
	   
	   if (debugOn) {
		   val filtRecordsArray = filtRecords.collect
	       filtRecordsArray.foreach(r => println(r))
	       println("invalid record count:" + invalidRecCount.value)
	   }
	   
	   if(saveOutput) {	   
		   filtRecords.saveAsTextFile(outputPath) 
		   
		   //if outputting valid recs, may need to output invalid recs also
		   if (outputValidRecs)  {
		     invalidRecsFilePath match {
		       case Some(path : String) => {
		    	   val invalidRecords = processedRecords.filter(r => {
		    		   r.contains(invalidRecordMarker) || r.contains(invalidFieldMarker)
		    	   })
		           invalidRecords.saveAsTextFile(path) 	   
		       }
		       case None => 
		     }
		   }
		   
   	   }
	   
   }
}