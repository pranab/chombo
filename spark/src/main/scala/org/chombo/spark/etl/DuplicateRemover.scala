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
import org.chombo.stats.CompleteStat
import org.chombo.util.BasicUtils

object DuplicateRemover extends JobConfiguration {
   
  /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "duplicateRemover"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val keyAttributes = getOptionalIntListParam(appConfig, "key.field.ordinals")
	   val KeyAttributeArray = keyAttributes match {
	     case Some(ords : java.util.List[Integer]) => Some(BasicUtils.fromListToIntArray(ords))
	     case None => None
	   }
	   val outpputMode = getStringParamOrElse(appConfig, "output.mode", "all")
	   val outputWholeRec = this.getBooleanParamOrElse(appConfig, "output.wholeRec", true)
	     
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   val data = sparkCntxt.textFile(inputPath)
	   
	   val keyedData = data.keyBy(r => {
	     val key = KeyAttributeArray match {
	       case Some(ords : Array[Int]) => {
	         val items = r.split(fieldDelimIn, -1)
	         BasicUtils.extractFields(items, ords, fieldDelimIn)}
	       case None => r
	     }
	     key
	   })

	   //dup removed
	   val dupRemData = keyedData.groupByKey.mapValues(vi => {
	     val va = vi.toArray
	     var rec = va.head
	     val size = va.length
	     rec = outpputMode match {
	       case "all" => rec
	       case "duplicate" => if (size > 1) rec else "x"
	       case "unique" => if (size == 1) rec else "x"
	     }
	     rec
	   }).filter(v => !v._2.equals("x"))
	   
	   val outputData = dupRemData.map(v => {
	     if (outputWholeRec) v._2 else v._1
	   })
	   
	   if (debugOn) {
         val records = outputData.collect
         records.foreach(r => println(r))
       }
	   
	   if(saveOutput) {	   
	     outputData.saveAsTextFile(outputPath) 
	   }

   }
   
   
}