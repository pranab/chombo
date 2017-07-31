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


package org.chombo.spark.explore

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.spark.common.Record
import org.chombo.stats.CategoricalHistogramStat

object CategoricalAttrDistrStats extends JobConfiguration {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "categoricalAttrDistrStats"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFieldOrdinals = getMandatoryIntListParam(appConfig, "id.field.ordinals").asScala.toArray
	   val catAttrOrdinals = getMandatoryIntListParam(appConfig, "cat.attr.ordinals", "").asScala.toArray
	   val extendedOutput = getBooleanParamOrElse(appConfig, "extended.output", true)
	   val outputPrecision = getIntParamOrElse(appConfig, "output.precision", 3);
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //key with record key and attr ordinal and value map of counts
	   val pairedData = data.flatMap(line => {
		   val items = line.split(fieldDelimIn, -1)
		   val keyRec = Record(items, keyFieldOrdinals)
		   val attrValCount = catAttrOrdinals.map(ord => {
		     val attrKeyRec = Record(keyRec.size + 1 ,keyRec)
		     attrKeyRec.addInt(ord)
		     
		     val attrValRec =new CategoricalHistogramStat()
		     attrValRec.
		     	withExtendedOutput(extendedOutput).
		     	withOutputPrecision(outputPrecision)
		     	
		     val attrVal = items(ord)
		     attrValRec.add(attrVal)
		     (attrKeyRec, attrValRec)		
		   })
		   attrValCount
	   })	   
	   
	   //merge histograms
	   val stats = pairedData.reduceByKey((h1, h2) => h1.merge(h2))

	   if (debugOn) {
	     val colStats = stats.collect
	     colStats.foreach(s => {
	       println("id:" + s._1)
	       println("distr:" + s._2)
	     })
	   }
	   
	   if (saveOutput) {
	     stats.saveAsTextFile(outputPath)
	   }
	   
   }

}