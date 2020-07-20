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
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import org.chombo.spark.common.GeneralUtility
import scala.collection.mutable.ArrayBuffer
import org.chombo.math.MathUtils

/**
* distance between pair of records
* @param args
* @return
*/
object NeighborDistance extends JobConfiguration with GeneralUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "neighborDistance"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFieldOrdinals = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val seckeyFieldOrdinal = getMandatoryIntParam(appConfig, "sec.key.field.ordinal","missing secondary key field ordinal") 
	   val attrOrds = BasicUtils.fromListToIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	   val distAlgo = getStringParamOrElse(appConfig, "dist.algo", "euclidean")
	   val distAlgoList = Array[String]("euclidean", "manhattan")
	   assertStringMember(distAlgo, distAlgoList, "invalid distance algorithm " + distAlgo)
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3);
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   //input
	   var data = sparkCntxt.textFile(inputPath)
	   
	   //keyed data
	   val keyedData =  getKeyedValue(data, fieldDelimIn, keyLen, keyFieldOrdinals)
	   
	   val pairDist = keyedData.groupByKey.flatMap(v => {
	     val key = v._1
	     val keyStr = key.toString
	     val values = v._2.toList
	     val size = values.length
	     val pairDist = ArrayBuffer[String]()
	     
	     for (i <- 0 to size-1) {
	       val line = values(i).getString(0)
	       val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	       val fRec = BasicUtils.extractFieldsAsDoubleArray(items , attrOrds)
	       val fKey = items(seckeyFieldOrdinal)
	       for (j <- i to size-1) {
	         val line = values(j).getString(0)
	         val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	         val sRec = BasicUtils.extractFieldsAsDoubleArray(items , attrOrds)
	         val sKey = items(seckeyFieldOrdinal)
	         val dist = distAlgo match {
	           case "euclidean" => MathUtils.euclideanDist(fRec, sRec)
	           case "manhattan" => MathUtils.manhattanDist(fRec, sRec)
	         }
	         
	         val rec = keyStr + fieldDelimOut + fKey + fieldDelimOut + sKey + fieldDelimOut + 
	         BasicUtils.formatDouble(dist, precision)
	         
	       }
	     }	     
	     pairDist
	   })
	   
	   if (debugOn) {
         val records = pairDist.collect
         records.slice(0, 20).foreach(r => println(r))
     }
	   
	   if(saveOutput) {	   
	     pairDist.saveAsTextFile(outputPath) 
	   }	 
   }
  
}