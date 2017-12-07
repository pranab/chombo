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


/**
 * Normlizes data according to various strategies. For zScore normalization, optionally removes
 * outliers
 * @author pranab
 *
 */
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
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val attrConfigs = appConfig.getConfigList("num.attrs").asScala
	   val attrConfigDetails = attrConfigs.map(c => {
	     (c.getInt("ordinal"), c.getString("type"))
	   })
	   val outlierTruncLevel = this.getOptionalDoubleParam(appConfig, "outlier.trunc.level")
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val normStrategy = getStringParamOrElse(appConfig, "norm.strategy", "zScore")
	   val scale = this.getOptionalIntParam(appConfig, "scale")
	   val forceUnitRangeForZscore = getBooleanParamOrElse(appConfig, "force.unit.range.for.zscore", true)
	   
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   val data = sparkCntxt.textFile(inputPath)
	   data.cache
	   
	   //filed ordinal and value
	   val fieldVals = data.flatMap(line => {
	     val items = line.split(fieldDelimIn)
	     val allFields = attrConfigDetails.map(attr => {
	       val ord = attr._1
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
	   
	   //field wise stats
	   val fieldWiseStaats =  fieldVals.combineByKey(createStat, addToStat, mergeStats)
       val fieldStats = fieldWiseStaats.collectAsMap
       
       //normalize data
       val normalized = data.map(line => {
         val items = line.split(fieldDelimIn)
         
         var outlier = false
         attrConfigDetails.foreach(attr => {
           val attrOrd = attr._1
           val attrType = attr._2
           val curVal = items(attrOrd).toDouble 
           val stat = fieldStats.get(attrOrd)
           val realStat = stat match {
             case Some(realStat: CompleteStat) => realStat
             case None => throw new IllegalStateException("missing stat")
           }
           
           //apply strategies
           var normValue = normStrategy match {
             case "minMax" => {
               (curVal - realStat.getMin()) / (realStat.getMax() - realStat.getMin())
             }
             case "max" => {
               val absMax = BasicUtils.max(Math.abs(realStat.getMax()), Math.abs(realStat.getMin()))
               curVal / absMax
             }
             case "zScore" => {
               var znVal = (curVal - realStat.getMean()) / realStat.getStdDev()
               val znValAbs = Math.abs(znVal)
               outlier = outlierTruncLevel match {
                 case Some(truncLevel : Double) => { 
                   znVal = znVal / truncLevel
                   if (forceUnitRangeForZscore) znVal = (znVal + 1) / 2
                   outlier || znValAbs > truncLevel}
                 case None => false
               }
               znVal
             }
             case "center" => {
               curVal - realStat.getMean()
             }
             case "decimal" => {
               val absMax = BasicUtils.max(Math.abs(realStat.getMax()), Math.abs(realStat.getMin()))
               val maxLog = Math.log10(absMax)
               val pwr = (maxLog + 1).toInt
               curVal / Math.pow(10, pwr)
             }
             case "unitSum" => {
               curVal / realStat.getSum()
             }
           }
           
           //scale if necessary
           normValue = scale match {
             case Some(s : Int) => s * normValue
             case None => normValue
           }
           
           //string value
           val serVal = attrType match {
             case "int" => "" + Math.round(normValue)
             case "long" => "" + Math.round(normValue)
             case "float" => BasicUtils.formatDouble(normValue, precision)
             case "double" => BasicUtils.formatDouble(normValue, precision)
           }
           items(attrOrd) = serVal
         })
         
         val rec = outlier match {
           case true => "x"
           case false => items.mkString(fieldDelimOut)
         }
         rec
       })
	   
	   //filter out outliers
       val norlaizedFiletered = outlierTruncLevel match {
         case Some(truncLevel : Double) => normalized.filter(r => !r.equals("x"))
         case None => normalized
       }       
       if (debugOn) {
         val records = norlaizedFiletered.collect
         records.foreach(r => println(r))
       }
	   
	   if(saveOutput) {	   
	     norlaizedFiletered.saveAsTextFile(outputPath) 
	   }
   }
}