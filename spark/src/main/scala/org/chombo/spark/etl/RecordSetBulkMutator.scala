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
import org.apache.spark.util.LongAccumulator
import org.chombo.util.BasicUtils
import org.chombo.spark.common.Record
import scala.collection.mutable.ArrayBuffer

/**
 * bulk data mutation
 * @author pranab
 */
object RecordSetBulkMutator extends JobConfiguration {

   /**
  * @param args
  * @return
  */
   def main(args: Array[String]) {
	   val appName = "recordSetBulkMutator"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out",  ",")
	   val syncMode = getStringParamOrElse(appConfig, "sync.mode",  "partial")
	   val mutOp = getOptionalStringParam(appConfig, "mutation.op")
	   val incrFilePath = getMandatoryStringParam(appConfig, "incr.filePath", "missing incremental file path")
	   val keyFieldsOrdinals = getMandatoryIntListParam(appConfig, "id.fieldOrdinals", "missing key field ordinals").asScala.toArray
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrdinal", "missing sequence filed ordinal")
	   val maintainVersion = getBooleanParamOrElse(appConfig, "maintain.version", false)
	   val verOutputPath = getOptionalStringParam(appConfig, "versioned.filePath")
	   if (maintainVersion) {
	     verOutputPath match {
	       case Some(path : String ) => 
	       case None => throw new IllegalStateException("missing verioned data output path")
	     }
	   }
	   
	   val inUpsert = mutOp match {
	     case Some(op:String) => op.equals("upsert")
	     case None => syncMode.equals("partial")
	   }
	   
	   val baseRecPrefix = "$base"
	   val incrRecPrefix = "$incr"
	   val delRecPrefix = "$del"
	   val versionPrefix = "$ver"
	   val prefixLen = baseRecPrefix.length()
	   val verPrefLen = versionPrefix.length()
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)

	   val updateCounter = new LongAccumulator()
	   val insertCounter = new LongAccumulator()
	   val deleteCounter = new LongAccumulator()
	   
	   //base and incremental data
	   val baseData = sparkCntxt.textFile(inputPath)
	   val keyedBaseRecs =  getKeyedRecs(baseData, fieldDelimIn, keyFieldsOrdinals, mutOp, baseRecPrefix)
	   keyedBaseRecs.cache
	   
	   //val baseCount = keyedBaseRecs.count
	   
	   //incremental keyed data
	   val incrData = sparkCntxt.textFile(incrFilePath)
	   val keyedIncrRecs =  getKeyedRecs(incrData, fieldDelimIn, keyFieldsOrdinals, mutOp, incrRecPrefix)
	   
	   //val incCount = keyedIncrRecs.count
	   
	   //merge
	   val keyedRecs = (keyedBaseRecs ++ keyedIncrRecs).groupByKey
	   
	   //val totCount = keyedRecs.count
	   //println("baseCount=" + baseCount + " incCount=" + incCount + " totCount=" + totCount)

	   val updatedRecs = 
	     mutOp match {
	       case Some(op:String) => {
	         //mutation operation specified
		     val recs = 
		     if (op.equals("upsert")) {
			   //insert and update
	           keyedRecs.flatMap(v => {
	             val values = ArrayBuffer[String]()
	             val recs = v._2.toSeq
	             recs.sortBy(line => {
	               //descending order
	               val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	               -fields(seqFieldOrd).toLong
	             })
	             if (recs.size == 1) {
	               insertCounter.add(1)
	             } else {
	               updateCounter.add(1)
	             }
	             val r = recs(0)
	             values += r
	             for (i  <- 1 to recs.length - 1) {
	                 val v = versionPrefix + recs(i).substring(prefixLen)
	                 values += v
	                 //println("versioned rec")
	             }
	             values
	           })
	         } else {
	           //delete
	           deleteCounter.add(1)
	           keyedRecs.filter(v => v._2.toSeq.length == 1).map(v => v._2.toSeq(0))
	         }
		     recs
	       }
	       case None => {
	         //automatic
	         val recs = keyedRecs.flatMap(v => {
	           val values = ArrayBuffer[String]()
	           val recs = v._2.toList
	           if (recs.length > 1) {
	             //multiple recs per key, update or duplicate for full synchronization
	             recs.sortBy(line => {
	               //descending order
	               val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	               -fields(seqFieldOrd).toLong
	             })
	             updateCounter.add(1)
	             
	             //recent
	             val v = recs(0).substring(prefixLen)
	             values += v
	             
	             if (maintainVersion) {
	               //versioned for update
	               if (syncMode.equals("partial")){
		               for (i  <- 1 to recs.length - 1) {
		                 val v = versionPrefix + recs(i).substring(prefixLen)
		                 values += v
		                 //println("versioned rec")
		               }
	               }
	               values
	             } else {
	              values
	           	}
	           } else {
	             //one record per key insert or delete
	             val prefix = recs(0).substring(0, prefixLen)
	             val rec = recs(0).substring(prefixLen)
	             if (prefix.equals(baseRecPrefix)) {
	               if (syncMode.equals("partial")) {
	                 //leave alone
	                 values += rec
	                 values
	               } else {
	                 //delete
	            	 deleteCounter.add(1)                
	            	 val r = delRecPrefix + rec
	                 values += r
	                 values
	               }
	             } else {
	               //insert
	               insertCounter.add(1)
	               values += rec
	               values
	             }
	           }
	         })
	         recs.filter(r => !r.startsWith(delRecPrefix))
	       }
	   }
	   updatedRecs.cache
	   
	   //normal records
	   val normRecs = if (maintainVersion && inUpsert) updatedRecs.filter(r => !r.startsWith(versionPrefix)) else updatedRecs
	   
	   //versioned records
	   if (maintainVersion && inUpsert) {
	     verOutputPath match {
	       case Some(outputPath : String) => {
	         val verRecs = updatedRecs.filter(r => r.startsWith(versionPrefix)).map(r => r.substring(verPrefLen))
	         if (debugOn) {
	        	 println("versioned records")
	        	 verRecs.collect.slice(0, 10).foreach(s => println(s))
	         }
	         verRecs.saveAsTextFile(outputPath)
	       }
	       case None => 
	     }
	   } 
	   
	  if (debugOn) {
	     println("normal records")
	     normRecs.collect.slice(0, 50).foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     normRecs.saveAsTextFile(outputPath)
	  }
	 
	  println("** counters **")
	  println("insert count " + insertCounter.value)
	  println("update or duplicate count " + updateCounter.value)
	  println("delete count " + deleteCounter.value)
	   
   }
   
   /**
   * @param data
   * @param fieldDelimIn
   * @param keyFieldsOrdinals
   * @param mutOp
   * @param recPrefix
   * @return
   */  
   def getKeyedRecs(data:RDD[String], fieldDelimIn:String, keyFieldsOrdinals:Array[Integer],  
     mutOp:Option[String], recPrefix:String) : RDD[(Record, String)] = {
     data.map(line => {
		val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		val key = Record(fields, keyFieldsOrdinals)
		val value = mutOp match {
		    case Some(op:String) => line
		    case None => recPrefix + line
		}
		(key, value)
	 })
   }
}