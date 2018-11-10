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

/**
* Cross correlation. The 2 time series co related are in the data set as 2 columns
* @param args
* @return
*/
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
	   val keySize = keyFieldOrdinals.size
	   
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
	  
	  val stdDefFldOrd = statsKeyLen + getMandatoryIntParam(appConfig, "stdDev.fieldOrd","missing std deviation field ordinal")
	  val stdDevValueMap = BasicUtils.getKeyedValues(statsPath, statsKeyLen, stdDefFldOrd)
	  
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
		       
		       val fVal = Record(3)
		       var fld = numAttrOrdinals(0)
		       fVal.addInt(fld)
		       fVal.addInt(seq)
		       fVal.addDouble(items(fld).toDouble)
		       
		       val sVal = Record(3)
		       fld = numAttrOrdinals(1)
		       sVal.addInt(fld)
		       sVal.addInt(seq)
		       sVal.addDouble(items(fld).toDouble)
		       
		       val values = Array[Record](fVal, sVal)
		       for (key <- keys)
		         for (value <- values)
		        	 recs += ((lKey, value))
		   })		   
		   recs
	  })
	   
	  //cross corelate
	  var corRecs = keyedData.groupByKey.flatMap(r => {
	    val key = r._1
	    val va = r._2.toArray
	    val results =  ArrayBuffer[(Record, Double)]()
	    if (va.length == 4) {
	      va.sortBy(r => (r.getInt(0), r.getInt(1)))
	      
	      //lag
	      var fv = va(0)
	      var sv = va(3)
	      results += corResult(fv, sv, key, keySize, fieldDelimIn, meanValueMap, stdDevValueMap)
	      
	      //lead
	      fv = va(1)
	      sv = va(2)
	      results += corResult(fv, sv, key, keySize, fieldDelimIn, meanValueMap, stdDevValueMap)
	    } else {
	      val key = Record(1)
	      key.addInt(0)
	      val r = (key, 1.0)
	      results += r
	    }
	    results
	  })
	  
	  //aggregate
	  corRecs = corRecs.filter(r => r._1.size > 1).reduceByKey((v1,v2) => v1+ v2)

	  if (debugOn) {
	     corRecs.collect.foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     corRecs.saveAsTextFile(outputPath)
	  }
	  
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
   
   /**
 * @param size
 * @param record
 * @param subSize
 * @param shift
 * @return
 */
  def buildCorKey(size:Int, record:Record, subSize:Int, shift:Int) : Record = {
     val newKey = Record(size, record, 0, subSize)
     newKey.addInt(shift)
     newKey
   }
   
   /**
 * @param fv
 * @param sv
 * @param key
 * @param keySize
 * @param fieldDelimIn
 * @param meanValueMap
 * @return
 */
  def corResult(fv:Record, sv:Record, key:Record, keySize:Int, fieldDelimIn:String,  
       meanValueMap:java.util.Map[String,java.lang.Double], stdDevValueMap:java.util.Map[String,java.lang.Double]) : (Record, Double) = {
      val shift = fv.getInt(1) - sv.getInt(1)
      val cKey = buildCorKey(keySize + 1, key, keySize, shift)
      
      val fStatsKey = key.toString(0, keySize) + fieldDelimIn + fv.getInt(0)
      val fMean = meanValueMap.get(fStatsKey)
      val fStdDev = stdDevValueMap.get(fStatsKey)
      
      val sStatsKey = key.toString(0, keySize) + fieldDelimIn + sv.getInt(0)
      val sMean = meanValueMap.get(sStatsKey)
      val sStdDev = stdDevValueMap.get(sStatsKey)
      
      val cValue = ((fv.getDouble(2) - fMean) * (sv.getDouble(2) - sMean)) / (fStdDev * sStdDev)
      (cKey, cValue)
   }
   
}