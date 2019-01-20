/*
 `* chombo-spark: etl on spark
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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils

/**
 * @param keyFields
 * @return
 */
object UniqueCompositeValueConunter extends JobConfiguration with GeneralUtility  {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "uniqueCompositeValueConunter"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFields = getMandatoryIntListParam(appConfig, "id.fieldOrdinals", "missing id field ordinals")
	   val keyFieldOrdinals = toIntArray(keyFields)
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   val keyLen =  keyFieldOrdinals.size
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   val countedRecs = data.map(line => {
	     val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		 val key = Record(keyLen)
		 populateFields(fields, keyFieldOrdinals, key)
		 (key, 1)
	   }).reduceByKey((v1, v2) => v1 + v2)
	   
	  if (debugOn) {
	     countedRecs.collect.slice(0,100).foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     countedRecs.saveAsTextFile(outputPath)
	  }

   }
}