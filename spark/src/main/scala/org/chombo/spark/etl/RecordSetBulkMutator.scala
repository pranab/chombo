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
import org.chombo.util.BasicUtils
import org.chombo.spark.common.Record

/**
 * bulk data mutation
 * @author pranab
 */
object RecordSetBulkMutator extends JobConfiguration {

   /**
  * @param args
  * @return
  */
   def main(args: Array[String]) {
	   val appName = "normalizer"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out",  ",")
	   val mutOp = getStringParamOrElse(appConfig, "mutation.op", "upsert")
	   val incrFilePath = getMandatoryStringParam(appConfig, "incr.filePath", "missing incremental file path")
	   val keyFieldsOrdinals = getMandatoryIntListParam(appConfig, "id.fieldOrdinals", "missing key field ordinals").asScala.toArray
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrdinal", "missing sequence filed ordinal")
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   //base and incremental data
	   val baseData = sparkCntxt.textFile(inputPath).cache
	   val inccData = sparkCntxt.textFile(incrFilePath)
	   val totData = baseData ++ inccData
	   
	   //keyed data
	   val keyedRecs = totData.map(line => {
		   val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val key = Record(fields, keyFieldsOrdinals)
		   (key, line)
	   }).groupByKey
	   
	   val updatedRecs = 
	   if (mutOp.equals("upsert")) {
	     //insert and update
	     keyedRecs.map(v => {
	       val recs = v._2.toSeq
	       recs.sortBy(line => {
	         val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	         -fields(seqFieldOrd).toLong
	       })
	       recs(0)
	    })
	   } else {
	     //delete
	     keyedRecs.filter(v => v._2.toSeq.length == 1).map(v => v._2.toSeq(0))
	   }
	   
	  if (debugOn) {
	     updatedRecs.collect.foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     updatedRecs.saveAsTextFile(outputPath)
	  }
	   
   }
}