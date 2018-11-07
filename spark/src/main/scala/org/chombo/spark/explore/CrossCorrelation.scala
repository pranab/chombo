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
import org.chombo.util.SeasonalAnalyzer
import org.chombo.spark.common.SeasonalUtility
import org.chombo.util.BasicUtils
import scala.collection.mutable.ArrayBuffer
import org.chombo.spark.common.GeneralUtility

object CrossCorrelation extends JobConfiguration with GeneralUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "crossCorrelation"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrdinal", "missing sequence filed ordinal")
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = keyFields match {
	     case Some(fields:java.util.List[Integer]) => Some(fields.asScala.toArray)
	     case None => None  
	   }
	  val numAttrOrdinals = getMandatoryIntListParam(appConfig, "attr.ordinals", 
	      "missing quant attribute ordinals").asScala.toArray
	  val corrLags = getMandatoryIntListParam(appConfig, "coor.lags", "missing correlation lags").asScala.toArray
	  val outputPrecision = getIntParamOrElse(appConfig, "output.precision", 3);

	  //key length
	  var keyLen = getKeyLen(keyFieldOrdinals)
	  var keyDefined = keyLen == 0
	  keyLen += 3
	  
	  //mean values from stats output file
	  val statsPath = getMandatoryStringParam(appConfig, "stats.file.path", "missing stat file path")
	  var statsKeyLen = keyLen - 4
	  statsKeyLen += 1
	  val meanFldOrd = statsKeyLen + getMandatoryIntParam(appConfig, "mean.fieldOrd","missing mean field ordinal")
	  val meanValueMap = BasicUtils.getKeyedValues(statsPath, statsKeyLen, meanFldOrd)
	  
	  val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	  val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	  //input
	  val data = sparkCntxt.textFile(inputPath)
 
	  //replace ts field with seq
	  val seqData = replTimestampWithSeq(data, fieldDelimIn, keyDefined, keyLen, keyFieldOrdinals, seqFieldOrd)
	  
	  //key with id fields, quant field, lag, seq pair
	  val keyedData = seqData.flatMap(line => {
		   val items = line.split(fieldDelimIn, -1)
		   val seq = items(seqFieldOrd).toInt
		   val recs = ArrayBuffer[(Record, Record)]()
		     
		   //each lag
		   corrLags.foreach(lag => {
		       val laggedSeq = seq - lag
		       val aheadSeq = seq + lag
		       val lKey = buildKey(lag, laggedSeq, seq, keyLen, keyFieldOrdinals, items)
		       val aKey = buildKey(lag, seq, aheadSeq, keyLen, keyFieldOrdinals, items)
		       val keys = Array[Record](lKey, aKey)
		       
		       val fVal = Record(2)
		       var fld = numAttrOrdinals(0)
		       fVal.addInt(fld)
		       fVal.addDouble(items(fld).toDouble)
		       
		       val sVal = Record(2)
		       fld = numAttrOrdinals(1)
		       sVal.addInt(fld)
		       sVal.addDouble(items(fld).toDouble)
		       
		       val values = Array[Record](fVal, sVal)
		       for (key <- keys)
		         for (value <- values)
		        	 recs += ((lKey, value))
		   })		   
		   
		   recs
	  })
	   
   }
   
   /**
   * @param appName
   * @param config
   * @param includeAppConfig
   * @return
   */ 
   def buildKey(lag:Integer, firstSeq: Int, secondSeq: Int, keyLen:Int, keyFieldOrdinals:Option[Array[Integer]], 
      items: Array[String]) : Record = {
       val key = Record(keyLen)
       Record.populateFields(items, keyFieldOrdinals, key)
       key.addInt(lag)
       key.addInt(firstSeq)
       key.addInt(secondSeq)
       key
   }
   

}