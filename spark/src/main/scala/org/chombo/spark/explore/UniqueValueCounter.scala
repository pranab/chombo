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
import org.chombo.util.BasicUtils

/**
 * Finds unique values column wise
 * @author pranab
 *
 */
object UniqueValueCounter extends JobConfiguration {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "uniqueValueCounter"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val catFieldOrdinals = getMandatoryIntListParam(appConfig, "cat.fieldOrdinals").asScala.toArray
	   val uniqueValCount = getBooleanParamOrElse(appConfig, "count.values", false)
	   val compUniqueValCount = getConditionalMandatoryBooleanParam(uniqueValCount, appConfig, "count.compValues", 
	       "missing copisite unique value count flag")
	   val caseInsensitive = getBooleanParamOrElse(appConfig, "case.insensitive", false)
	   val completenessfactor = getConditionalMandatoryBooleanParam(uniqueValCount, appConfig, "completeness.factor", 
	       "missing completeness flag")
	   val cardinality = if (completenessfactor) {
	     getMandatoryIntIntMapParam(appConfig, "field.cardinality", "missing field cardinality parameter").asScala
	   } else {
	     (new java.util.HashMap[Int, Int]).asScala
	   }
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)

	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   val serUniqueData = if (uniqueValCount) {
	     //unique value count
		   var colValues = data.flatMap(line => {
			   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
			   if (compUniqueValCount) {
			     val key = Record(items, catFieldOrdinals)
			     val value = (key, 1)
			     Array.fill[(Record, Int)](1)(value)
			   } else {
			     catFieldOrdinals.map(i => {
			       val colValue = items(i)
			       val key = Record(2)
			       key.add(i,colValue)
			       (key, 1)
			     })
			   }
		   }).reduceByKey((v1, v2) => v1 + v2)	
		   
		   if (!compUniqueValCount) {
		     //from per unique value count to all value value count
			   var colValCount = colValues.map(r => {
			     val key = r._1
	         val newKey = Record(key.size - 1, key)
	         (newKey, 1)
		     }).reduceByKey((v1, v2) => v1 + v2).mapValues(v => {
		       val valRec = Record(1)
		       valRec.addInt(v)
		       valRec
		     })	
		     if (completenessfactor) {
		       //distribution completeness factor
		       colValCount = colValCount.map(r => {
		         val keyRec = r._1
		         val count = r._2.getInt(0)
		         val card = cardinality.get(keyRec.getInt(0)).get
		         val compFac = count.toDouble / card
		         val valRec = Record(2)
		         valRec.addInt(count)
		         valRec.addDouble(compFac)
		         (keyRec, valRec)
		       })
		     }
			   colValCount.map(r => r._1.toString + fieldDelimOut + r._2.withFloatPrecision(precision).toString)
		   } else {
		     colValues.map(r => r._1.toString + fieldDelimOut + r._2)
		   }
	   } else {
	      //unique values
	      val colValues = data.flatMap(line => {
		    val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		    catFieldOrdinals.map(i => {
		      val colIndex = i.toInt
		      var colValue = items(colIndex)
		      colValue = if (caseInsensitive) colValue.toLowerCase() else colValue
		      val colValSet = Set[String](colValue)
		      (colIndex, colValSet)
		     })
	     }).reduceByKey((v1, v2) => v1 ++ v2)
	     colValues.map(r => "" + r._1 + fieldDelimOut + r._2.mkString(fieldDelimOut))
	   }
	   
	   
	   if (debugOn) {
	     val uniqueValues = serUniqueData.collect
		   uniqueValues.foreach(line => println(line))
	   }
		   
	   if (saveOutput) {
	     serUniqueData.saveAsTextFile(outputPath)
	   }
	   
   }
}