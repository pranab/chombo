/*
 `* chombo-spark: etl on spark
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
import scala.collection.mutable.ArrayBuffer
import org.chombo.spark.common.Record
import org.chombo.util.SeasonalAnalyzer
import org.chombo.spark.common.SeasonalUtility
import org.chombo.util.BasicUtils
import org.chombo.stats.NumericalAttrStatsManager
import org.chombo.spark.common.GeneralUtility

/**
 * Mean value vector and co variance matrix
 * @param args
 * @return
 */
object Covariance extends JobConfiguration with SeasonalUtility with GeneralUtility{
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "covariance"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFieldOrdinals = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val numAttrOrdinals = getMandatoryIntListParam(appConfig, "attr.ordinals", 
	      "missing quant attribute ordinals").asScala.toArray
	   val numAttrOrdinalsIndx = numAttrOrdinals.zipWithIndex
	   val dimension = numAttrOrdinals.size
	   
	   
	   //seasonal data
	   val seasonalAnalysis = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)
	   val seasonalAnalyzers =  creatOptionalSeasonalAnalyzerArray(this, appConfig, seasonalAnalysis)
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, seasonalAnalysis, 1)
	   
	   val outputPrecision = getIntParamOrElse(appConfig, "output.precision", 3);
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   
	   val data = sparkCntxt.textFile(inputPath)
	   val keyedRecs = data.flatMap(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val fieldStats = numAttrOrdinalsIndx.map(attr => {
		     val attrOrd = attr._1
		     val indx = attr._2
		     val key = Record(keyLen)
		     
		     //partioning fields
		     populateFields(items, keyFieldOrdinals, key)
		     
		     //seasonality cycle related fields
		     addSeasonalKeys(seasonalAnalyzers, items, key)
		     
		     //attr ordinal
		     key.addInt(attrOrd)
		     
		     //value
		     val quantVal = items(attrOrd).toDouble
		     val remaining = numAttrOrdinals.length - 1 - indx
		     val value = Record(3 + remaining)
		     value.addInt(1)
		     value.addDouble(quantVal)
		     value.addDouble(quantVal * quantVal)
		     
		     //cross terms
		     for (i <- (indx+1) to (numAttrOrdinals.length -1)) {
		       val attrIndx = numAttrOrdinals(i)
		       val nextQuantVal = items(attrIndx).toDouble
		       value.addDouble(quantVal * nextQuantVal)
		     }
		     
		     (key,value)
		   })
		   fieldStats
	  })	
	  
	  //aggregate
	  var aggrRecs = keyedRecs.reduceByKey((v1, v2) => {
	    val sz = v1.size
	    val aggr = Record(sz)
	    
	    //count
	    var i = 0
	    aggr.addInt(v1.getInt(i) + v2.getInt(i))
	    i += 1
	    
	    //sum
	    aggr.addDouble(v1.getDouble(i) + v2.getDouble(i))
	    i += 1
	    
	    //sum sq
	    aggr.addDouble(v1.getDouble(i) + v2.getDouble(i))
	    i += 1
	    
	    //cross terms
	    for (j <- i to (sz - 1)) {
	      aggr.addDouble(v1.getDouble(j) + v2.getDouble(j))
	    }
	    aggr
	  })
	  
	  //move quant filed ord from key to value
	  aggrRecs = aggrRecs.map(v => {
	    val key = v._1;
	    val value = v._2
	    val newKey = if (key.size > 1) {
	    	val newKey = Record(key, 0, key.size - 1)
	    	newKey
	  	} else {
	  		val newKey = Record(1)
	  		newKey.addString("all")
	  		newKey
	  	}
	    val quantFldOrd = key.getInt(key.size - 1)
	    val newValue = Record(value.size+1, value, 1)
	    newValue.addInt(0, quantFldOrd)
	    (newKey, newValue)
	  })
	  
	  //mean vector and covariance matrix
	  val keyedStats = aggrRecs.groupByKey().map(r => {
	    val key = r._1
	    val values = r._2.toArray.sortBy(v => v.getInt(0)).zipWithIndex
	    val meanVec = Array[Double](dimension)
	    val coVarMat = Array.ofDim[Double](dimension,dimension) 
	    
	    //mean and variance
	    values.foreach(v => {
	      val aggr = v._1
	      val indx = v._2
	      
	      var i = 1
	      val count = aggr.getInt(i)
	      i += 1
	      val sum = aggr.getDouble(i)
	      i += 1
	      val sumSq = aggr.getDouble(i)
	      i += 1
	      
	      val mean = sum / count
	      meanVec(indx) = mean
	      val vari = sumSq / (count -1) - mean * mean
	      coVarMat(indx)(indx) = vari
	    })
	    
	    //covariance
	    values.foreach(v => {
	      val aggr = v._1
	      val i = v._2
	      val count = aggr.getInt(1)
	      var k = i + 1
	      for (j <- 4 to (dimension - 1)) {
	        val coVar = aggr.getDouble(j) / count - meanVec(i) * meanVec(j)
	        coVarMat(i)(k) =  coVar
	        coVarMat(k)(i) = coVar
	        k += 1
	      }
	    })	    
	    
	    val stat = (meanVec, coVarMat)
	    (key, stat)
	    
	  })
	  
	  //serialize,
	  val serKeyedStats =  keyedStats.flatMap(r => {
	    val key = r._1
	    val meanVec = r._2._1
	    val coVarMat = r._2._2
	    val serArr = ArrayBuffer[String]()
	    serArr += key.toString()
	    serArr += BasicUtils.join(meanVec, fieldDelimOut, outputPrecision)
	    coVarMat.foreach(r => {
	      serArr += BasicUtils.join(r, fieldDelimOut, outputPrecision)
	    })
	    serArr
	  })
	  
    if (debugOn) {
       val records = serKeyedStats.collect.slice(0,20) 
       records.foreach(r => println(r))
    }
	   
	   if(saveOutput) {	   
	     serKeyedStats.saveAsTextFile(outputPath) 
	   }
	  
  }

}