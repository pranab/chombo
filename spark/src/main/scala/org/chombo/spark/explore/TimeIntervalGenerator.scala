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
import org.chombo.util.BasicUtils
import org.chombo.util.SeasonalAnalyzer
import com.typesafe.config.Config
import org.chombo.spark.common.Record
import org.chombo.spark.common.GeneralUtility
import scala.collection.mutable.ArrayBuffer

/**
 * Replaces time stamp with time interval 
 * @author pranab
 *
 */
object TimeIntervalGenerator extends JobConfiguration with GeneralUtility {
   
  /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "timeIntervalGenerator"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFields = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val timeStampFieldOrdinal = getMandatoryIntParam(appConfig, "time.fieldOrdinal", 
	       "missing time stamp field ordinal")
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)

	   //input
	   val data = sparkCntxt.textFile(inputPath)	
	   val transData = data.map(line => {
	     val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	     val key = Record.createFromArrayWithDefault(fields, keyFields, "all")
	     (key, fields)
	   }).groupByKey.flatMapValues(it => {
	       val values = it.toArray
	       val soValues = values.sortBy(v => v(timeStampFieldOrdinal).toLong)
	       val upValues = ArrayBuffer[Array[String]]()
	         
	       for (i <- 1 to (soValues.length - 1)) {
	         val diff = soValues(i)(timeStampFieldOrdinal).toLong - soValues(i-1)(timeStampFieldOrdinal).toLong
	         val size = soValues(i).length
	         val newRec = new Array[String](size)
	         Array.copy(soValues(i), 0, newRec, 0, size)
	         newRec(timeStampFieldOrdinal) = diff.toString()
	         upValues += newRec
	       }
	       val lines = upValues.map(r => r.mkString(fieldDelimOut)).toArray
	       lines
	   }).values
	   
	  if (debugOn) {
	     transData.collect.slice(0,50).foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     transData.saveAsTextFile(outputPath)
	  }
	   
   }
}