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

package org.chombo.spark.sanity

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.spark.common.Record
import org.chombo.spark.common.RecordBasePartitioner


object SecondarySorting extends JobConfiguration {
   
  /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "secondarySorting"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val keyFields = getMandatoryIntListParam(appConfig, "id.fieldOrdinals").asScala.toArray
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   val sortFields = Array[Int](1)
	   sortFields(0) = 1
	   
	  val keyLen = keyFields.length	

	  //input
	  val data = sparkCntxt.textFile(inputPath)
	   
	 val keyedData = data.map(line => {
	   val items = line.split(fieldDelimIn, -1)
	   val key = Record(keyLen)
	   //partitioning fields
       for (kf <- keyFields) {
    	   key.addString(items(kf))
       }
	   (key, line)
	 })
	 println("keyed data")
	 
	 keyedData.foreach(r => {
	   r._1.withSortFields(sortFields)
	   r._1.withSecondaryKeySize(1)
	 })
	 println("keyed data prepared")
	 
	 var keyedDataSorted = keyedData.repartitionAndSortWithinPartitions(new RecordBasePartitioner(2))
	 
	 keyedDataSorted.foreachPartition(p => {
	   val l = p.toList
	   var curKey = ""
	   l.foreach(v => {
	     val items = v._2.split(fieldDelimIn, -1)
	     val key = items(0)
	     if (!key.equals(curKey)) {
	       val ts = items(1)
	       println(key + fieldDelimOut + ts)
	       curKey = key
	     }
	   })
	 })
	 
	 if (debugOn) {
         val records = keyedDataSorted.collect
         records.slice(0, 100).foreach(r => println(r))
     }
	 if(saveOutput) {	   
	     keyedDataSorted.saveAsTextFile(outputPath) 
	 }	 
  
   }

}