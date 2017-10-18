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


object Normalizer extends JobConfiguration {

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
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val attrOrdinals = getMandatoryIntListParam(appConfig, "num.attr.ordinals").asScala
	   val normStrategy = this.getMandatoryStringParam(appConfig, "norm.strategy")
	   
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   val data = sparkCntxt.textFile(inputPath)
	   data.cache
	   
	   //filed ordinal and value
	   val fieldVals = data.flatMap(line => {
	     val items = line.split(fieldDelimIn)
	     val allFields = attrOrdinals.map(ord => {
	      val fieldVal = items(ord).toDouble 
	      (ord, fieldVal)
	     })
	     allFields
	   })
	   
	   //stats for each field
	   val createStat = (v:Double) => {
	     val stat = new CompleteStat()
	     stat.add(v)
	     stat
	   }
	   
	   //add to stat
	   val addToStat = (stat:CompleteStat, v:Double) => {
	     stat.add(v)
	     stat
	   }
	   
	   //merge stat
	   val mergeStats = (thisStat : CompleteStat, thatStat:CompleteStat) => {
	     thisStat.merge(thatStat)
	     thisStat
	   }
	   
	   val fieldWiseStaats =  fieldVals.combineByKey(createStat, addToStat, mergeStats)
       val fieldStats = fieldWiseStaats.collectAsMap
       
       //normalize data
       data.map(line => {
         val items = line.split(fieldDelimIn)
         attrOrdinals.foreach(ord => {
           val curVal = items(ord).toDouble 
           val stat = fieldStats.get(ord)
           val realStat = stat match {
             case Some(realStat: CompleteStat) => realStat
             case None => throw new IllegalStateException("missing stat")
           }
           
           val normValue = normStrategy match {
             case "minMax" => {
               (curVal - realStat.getMin()) /(realStat.getMax() - realStat.getMin())
             }
             case "zscore" => {
               Math.abs((curVal - realStat.getMean()) / realStat.getStdDev())
             }
             case "center" => {
               curVal - realStat.getMean()
             }
             case "unitSum" => {
               
             }
           }
         })
         
       })
	   
	   


   }
}