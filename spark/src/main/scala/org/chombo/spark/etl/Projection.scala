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
import org.apache.spark.rdd.RDD
import org.chombo.util.BasicUtils
import org.chombo.spark.common.Record
import scala.collection.mutable.ArrayBuffer
import org.chombo.spark.common.GeneralUtility
import com.typesafe.config.Config
import org.chombo.util.RowColumnFilter
import org.chombo.util.AttributeFilter
import org.chombo.util.BaseAttributeFilter

/**
 * selection and projection
 * @author pranab
 *
 */
object Projection extends JobConfiguration with GeneralUtility {

   /**
  * @param args
  * @return
  */
   def main(args: Array[String]) {
	   val appName = "projection"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out",  ",")
	   val operation = getMandatoryStringParam(appConfig, "op.mode", "missing operation")
	   val groupBy = operation.startsWith("group")
	   val grByFields = toOptionalIntArray(getOptionalIntListParam(appConfig, "groupBy.fields"))
	   val orderByFiels = toOptionalStringArray(getOptionalStringListParam(appConfig, "groupBy.fields"))
	   grByFields match {
	     case Some(fields) => {
	       orderByFiels match {
	         case Some(fields) => BasicUtils.assertFail("can not have groupby and order by both")
	         case None =>
	       }
	     }
	     case None => 
	   }
	   
       val orderByfiledAndType = orderByFiels match {
         case Some(fields) => {
           val filedAndType = fields.map(v => {
             val items = v.split(":")
             (items(0).toInt, items(1))
           })
           Some(filedAndType)
         }
         case None => None
       }
	   val projectionFields = getMandatoryIntListParam(appConfig, "project.fields", "missing projection fields")
	   val selectionFilter = getMandatoryStringParam(appConfig, "selection.filter", "")
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   val rowColFilter = new RowColumnFilter()
	   val attrFilter = buildSelectFilter(appConfig, selectionFilter, rowColFilter)
	   
	   val data = sparkCntxt.textFile(inputPath).cache
	   val transformedData = data.filter(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   attrFilter.evaluate(items)
	   })	
	   
	   if (debugOn) {
         val records = transformedData.collect
         records.slice(0, 200).foreach(r => println(r))
       }
	   
	   if(saveOutput) {	   
	     transformedData.saveAsTextFile(outputPath) 
	   }	 
	   
   }
   
   /**
   * @param appName
   * @param config
   * @param includeAppConfig
   * @return
   */
   def buildSelectFilter(appConfig:Config, selectionFilter:String, rowColFilter:RowColumnFilter) : AttributeFilter =  {
     val notInSetName = getOptionalStringParam(appConfig, "notInSet.name")
     val inSetName = getOptionalStringParam(appConfig, "inSet.name")
     
     var attrFilter =  new AttributeFilter()
     notInSetName match {
       case Some(niSetName) => {
         createRowsContext(appConfig,  rowColFilter, attrFilter, selectionFilter, niSetName)
         attrFilter
       }
       case None => {
         inSetName match {
           case Some(iSetName) => {
	         createRowsContext(appConfig,  rowColFilter, attrFilter, selectionFilter, iSetName)
	         attrFilter
           }
           case None => {
             val udfContext = getUdfConfiguration(appConfig)
             if (udfContext.isEmpty()) {
               attrFilter = new AttributeFilter(selectionFilter)
             } else {
               attrFilter =new AttributeFilter(selectionFilter, udfContext)
             }
             
           }
           
         }
         
       }
     }
     attrFilter
   }
   
   /**
   * @param list
   * @param defLen
   * @return
   */
   def getUdfConfiguration(appConfig:Config) : java.util.HashMap[String, Object] = {
     val udfContext = new java.util.HashMap[String, Object]()
     val udfConfigParams = getOptionalStringListParam(appConfig,"udf.configParams")
     udfConfigParams match {
       case Some(params) => {
         val paramList = params.asScala.toArray
         paramList.foreach(pName => {
           val pVal = getMandatoryStringParam(appConfig, pName)
           udfContext.put(pName, pVal)
         })
       }
       case None =>
     }
     udfContext
   }
   
   /**
   * @param appConfig
   * @param rowColFilter
   * @param attrFilter
   * @param selectionFilter
   * @param niSetName
   */
   def createRowsContext(appConfig:Config, rowColFilter:RowColumnFilter, attrFilter:BaseAttributeFilter,
       selectionFilter:String, niSetName:String) {
     val filterFieldDelim = getStringParamOrElse(appConfig, "filter.fieldDelim", ",")
     val excludeRowsFile = getMandatoryStringParam(appConfig, "exclude.rowsFile")
     val fiStrm = BasicUtils.getFileStream(excludeRowsFile)
     rowColFilter.processRows(fiStrm, filterFieldDelim)
     val exclRowKeys = rowColFilter.getExcludedRowKeys()
     val setOpContext = new java.util.HashMap[String, Object]()
     setOpContext.put(niSetName, BasicUtils.generateSetFromArray(exclRowKeys))
     attrFilter.withContext(setOpContext).build(selectionFilter)
   }
   
}