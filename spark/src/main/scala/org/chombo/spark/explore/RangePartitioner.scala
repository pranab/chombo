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

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils

/**
 * Range partitions. Adds new field with partition index
 * @author pranab
 *
 */
object RangePartitioner extends JobConfiguration  {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "numericalAttrStats"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val seqFieldOrdinal = getMandatoryIntParam(appConfig, "seq.fieldOrdinal", 
	       "missing seq field ordinal")
	   val rangeBoundaries = getMandatoryDoubleListParam(appConfig, "range.boundaries", "missing range definitions").
	   		asScala.toList.zipWithIndex
	   
	  val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	  val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	  
	  //input
	  val data = sparkCntxt.textFile(inputPath)	  
	  val partData = data.map(line => {
	    val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	    val seq = fields(seqFieldOrdinal).toDouble
	    val range = rangeBoundaries.find(r => seq < r._1)
	    range match {
	      case Some(r) => "" + r._2 + fieldDelimOut + line
	      case None => "" + rangeBoundaries.length + fieldDelimOut + line
	    }
	  }) 

	  if (debugOn) {
	     partData.collect.slice(0,50).foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     partData.saveAsTextFile(outputPath)
	  }
	  
   }
}