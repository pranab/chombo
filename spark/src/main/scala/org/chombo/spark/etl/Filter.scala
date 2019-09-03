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

import org.apache.spark.rdd.RDD
import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.Record
import org.chombo.rules.ParseTreeBuilder

/**
 * selection with complex expression and projection
 * @author pranab
 *
 */
object Filter extends JobConfiguration with GeneralUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String])  {
	   val appName = "filter"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFieldOrdinals = toIntArray(getMandatoryIntListParam(appConfig, "id.field.ordinals"))
	   val projectionFields = toOptionalIntArray(getOptionalIntListParam(appConfig, "project.fields"))
	   val selectionFilter = getMandatoryStringParam(appConfig, "selection.filter", "")
	   val statsPath = getOptionalStringParam(appConfig, "stats.file.path")
	   val keyLen = keyFieldOrdinals.length + 1
	   val meanFldOrd = keyLen + getIntParamOrElse(appConfig, "mean.fldOrd",4)
	   val stdDevFieldOrd = keyLen + getIntParamOrElse(appConfig, "stdDev.fldOrd",6)
	   val (meanMap, stdDevMap) = statsPath match {
	     case Some(path) => {
	       val meanMap = BasicUtils.getKeyedValues(path, keyLen, meanFldOrd)
	       val stdDevMap = BasicUtils.getKeyedValues(path, keyLen, stdDevFieldOrd)
	       (meanMap, stdDevMap)
	     }
	     case None => {
	       val meanMap = new java.util.HashMap[String,java.lang.Double]()
	       val stdDevMap = new java.util.HashMap[String,java.lang.Double]()
	       (meanMap, stdDevMap)
	     }
	   }
	   val schemaPath = getMandatoryStringParam(appConfig, "schema.file.path", "")
	   val schema = BasicUtils.getGenericAttributeSchema(schemaPath)
	   val attributes = schema.getAttributes()
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   val data = sparkCntxt.textFile(inputPath).cache
	   val transData = data.mapPartitions(it => {
	     val builder = new ParseTreeBuilder()
	     val evaluator = builder.buildParseTree(selectionFilter)
	     evaluator.withKeyedMeanValues(meanMap).withKeyedStdDevValues(stdDevMap).withAttributes(attributes)
	     it.map(r => {
	       val items = BasicUtils.getTrimmedFields(r, fieldDelimIn)
	       val key = BasicUtils.extractFields(items, keyFieldOrdinals, fieldDelimIn)
	       evaluator.withInput(items)
	       evaluator.withInputKey(key)
	       val selected = evaluator.evaluate().asInstanceOf[Boolean]
	       (r, selected)  
	     })
	   }, true).filter(r => r._2).map(r => {
	     projectionFields match {
	       case Some(fields) => {
	         val items = BasicUtils.getTrimmedFields(r._1, fieldDelimIn)
	         BasicUtils.extractFields(items, fields, fieldDelimIn)
	       }
	       case None => r._1
	     }
	   })

	   if (debugOn) {
         val records = transData.collect
         records.slice(0, 20).foreach(r => println(r))
       }
	   
	   if(saveOutput) {	   
	     transData.saveAsTextFile(outputPath) 
	   }	 

   }
}