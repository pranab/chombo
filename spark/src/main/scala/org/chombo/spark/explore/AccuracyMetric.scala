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
import org.apache.spark.util.LongAccumulator

/**
 * Calculate  accuracy metric comparing with reference master data
 */
object AccuracyMetric extends JobConfiguration with GeneralUtility {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String])  {
	   val appName = "accuracyMetric"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val targetKeyFields = toIntArray(getMandatoryIntListParam(appConfig, "id.fieldOrdinalsTarget"))
	   val masterKeyFields = toIntArray(getMandatoryIntListParam(appConfig, "id.fieldOrdinalsMaster"))
	   val targetFields = toIntArray(getMandatoryIntListParam(appConfig, "target.fieldOrdinals"))
	   val numFields = targetFields.length
	   val masterFields = toIntArray(getMandatoryIntListParam(appConfig, "master.fieldOrdinals"))
	   val fieldTypes = toStringArray(getMandatoryStringListParam(appConfig, "field.types", "missing field types"))
	   val targetFieldsTypes = targetFields.zip(fieldTypes)
	   val masterFieldsTypes = masterFields.zip(fieldTypes)
	   val fieldWeights = toDoubleArray(getMandatoryDoubleListParam(appConfig, "field.weights", 
	       "missing field weight list"))
	   val weightSum = fieldWeights.sum
	   val targetFilePath = getMandatoryStringParam(appConfig, "target.filePath", "missing file path")
	   val tagMetric = getBooleanParamOrElse(appConfig, "tag.metric", false)
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val missingMasterCounter = new LongAccumulator()
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   //target data
	   val targetData = sparkCntxt.textFile(targetFilePath)
	   val keyedTarget = targetData.map(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val keyRec =  Record(items, targetKeyFields)
		   val valRec = Record(targetFieldsTypes.length + 1)
		   Record.populateFields(items, targetFieldsTypes, valRec)
		   valRec.addString(line)
		   (keyRec, valRec)
	   }).cache
	   
	   //master data
	   val masterData = sparkCntxt.textFile(inputPath)
	   val keyedmaster = masterData.map(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val keyRec =  Record(items, masterKeyFields)
		   val valRec = Record.create(items, masterFieldsTypes)
		   (keyRec, valRec)
	   })

	   //all data
	   val recsWithAccuracy = (keyedTarget ++ keyedmaster).groupByKey().filter(r => {
	     val values = r._2.toArray
	     val targetRecOnly = values(0).size == numFields + 1
	     if (targetRecOnly) {
	       missingMasterCounter.add(1)
	     }
	     values.length == 2 || targetRecOnly
	   }).map(r => {
	     val values = r._2.toArray
	     var sum = 0.0
	     var complCount = 0
	     val rec = if (values.length == 2) {
	       for (i <- 0 to numFields - 1) {
	         if (values(0).getAny(i).equals(values(1).getAny(i))) {
	           sum += fieldWeights(i)
	           complCount += 1
	         }
	       }
	       sum /= weightSum
	       values.filter(v => v.size == numFields + 1)(0).getString(numFields)
	     } else {
	       sum = -0.001
	       values(0).getString(numFields)
	     }
	     val strBld = new StringBuilder(rec)
	     if (tagMetric) {
	       strBld.append(fieldDelimOut).append("acc")
	     }
	     strBld.append(fieldDelimOut).append(complCount).append(fieldDelimOut).
	       append(BasicUtils.formatDouble(sum, precision))
	     strBld.toString()
	   })
	   
     if (debugOn) {
       var records = recsWithAccuracy.collect
       records.foreach(r => println(r))
       println("missing master record count: " + missingMasterCounter.value)
     }
	   
	   if(saveOutput) {	   
	     recsWithAccuracy.saveAsTextFile(outputPath) 
	   }
	   
   }
}