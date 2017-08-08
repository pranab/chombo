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
import java.io.FileInputStream
import java.io.File
import org.codehaus.jackson.map.ObjectMapper
import org.chombo.transformer.RawAttributeSchema
import org.chombo.transformer.JsonFieldExtractor
import org.chombo.transformer.MultiLineJsonFlattener
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import org.chombo.transformer.JsonComplexFieldExtractor


object FlatRecordExtractorFromJson extends JobConfiguration {

   /**
 * @param args
 * @return
 */
   def main(args: Array[String]) {
	   val appName = "flatRecordExtractorFromJson"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val debugOn = appConfig.getBoolean("debug.on")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val schemaFilePath = appConfig.getString("schema.file.path")
       val mapper = new ObjectMapper();
       val rawSchema = mapper.readValue(new File(schemaFilePath), classOf[RawAttributeSchema])
	   val failOnInvalid = appConfig.getBoolean("fail.on.invalid")
	   val normalizeOutput = appConfig.getBoolean("normalize.output")
	   val idFieldPath = getOptionalStringParam(appConfig, "id.attr.path")
	   val idFieldPaths = getOptionalStringListParam(config, "id.attr.path")
	   val entityList = getOptionalStringListParam(appConfig, "entity.list")
	   val fieldExtractor = new JsonComplexFieldExtractor(failOnInvalid, normalizeOutput);
       fieldExtractor.setDebugOn(debugOn)
       idFieldPaths match {
         case Some(paths:java.util.List[String]) => {
           fieldExtractor.withIdFieldPaths(paths)
           if (debugOn)
        	   System.out.println("id path:" + paths)
         }
         case None => {
           if (normalizeOutput) {
        	   fieldExtractor.withAutoIdGeneration()
           }
           if(debugOn)
             System.out.println("no id path defined")
         }
       }
       val flattener = rawSchema.getRecordType() match {
         case RawAttributeSchema.REC_MULTI_LINE_JSON => {
           val flattener = new MultiLineJsonFlattener()
           flattener.setDebugOn(debugOn)
           Some(flattener)
         }
         case _ => None
       }
 	   val saveOutput = appConfig.getBoolean("save.output")
      
       
       val data = sparkCntxt.textFile(inputPath, 1)
       val invalidRecs = List("x")
       val transformedRecords = data.flatMap(line => {
    	   val jsonRecord = flattener match {
    	     //multi line JSON
    	     case Some(fl:MultiLineJsonFlattener) => {
    	    	 val rec = fl.processRawLine(line)
    	    	 val re = if (null != rec)
    	    	   Some(rec)
    	    	 else
    	    	   None
    	    	 re
    	       }
    	     //single line JSON
    	     case None => Some(line)
    	   }
    	   if (debugOn) {
    	     jsonRecord match {
    	       case Some(re:String) =>  println("got rec")
    	       case None => println("got no rec")
    	     }
    	   }
    	   
    	   val flatRecs = jsonRecord match {
    	     case Some(re:String) => {
    	       val listRecs = if (fieldExtractor.extractAllFields(re, rawSchema.getJsonPaths())) {
    	         val allRecs = normalizeOutput match {
    	           case false => {
    	             getDeNormalizedRecords(fieldExtractor, fieldDelimOut)
    	           }
    	           case true => {
    	             getNormalizedRecords(fieldExtractor, fieldDelimOut)
    	           }
    	         }
    	         allRecs
    	       } else {
    	         invalidRecs
    	       }
    	       listRecs
    	     }
    	     case None => invalidRecs
    	   }
    	   flatRecs.foreach(println(_))
         flatRecs
       })
	   
       //filter out invalid marked records
       val validRecs = transformedRecords.filter(!_.equals("x"))
       
       val outRecs = validRecs.collect
	   if (debugOn) {
	     outRecs.foreach(println(_))
	   }
	   
	   if(saveOutput) {	   
	     normalizeOutput match {
    	 	case false => {
    	 		//denormalized output
    	 		validRecs.saveAsTextFile(outputPath) 
    	    }
    	    case true => {
    	    	//normalized output
    	    	validRecs.cache
    	      
    	    	val entList = entityList match {
    	      		case Some(enList:java.util.List[String]) => enList
    	      		case None  => throw new IllegalStateException("missing entity list for denormalized output")
    	    	}
    	      
    	    	//filer parent and child entity type records
    	    	entList.asScala.foreach(ent => {
    	    		if (debugOn) {
    	    			System.out.println("next entity for output: " + ent)
    	    		}
    	    		val entityRecs = validRecs.filter(rec => rec.startsWith(ent))
    	    		val finalEntityRecs = entityRecs.map(rec => BasicUtils.splitOnFirstOccurence(rec, fieldDelimOut, true)(1))
    	    		val entOutPath = outputPath + "/" + ent
    	    		finalEntityRecs.saveAsTextFile(entOutPath) 
    	    	})
    	    }
    	 } 
   	   }
       
   }

   
   /**
   * @param fieldExtractor
   * @param fieldDelimOut
   * @return
   */
   def getDeNormalizedRecords(fieldExtractor : JsonComplexFieldExtractor, fieldDelimOut:String) : 
     List[String] = {
     val listOfArr = fieldExtractor.getExtractedRecords()
     println("num of flat records:" + listOfArr.size())
     
     //list of fields to list of records
     val recs = listOfArr.asScala.map(a => {
       a.mkString(fieldDelimOut)
     })
     recs.toList
   }
   
   /**
   * @param fieldExtractor
   * @param fieldDelimOut
   * @return
   */
   def getNormalizedRecords(fieldExtractor : JsonComplexFieldExtractor, fieldDelimOut:String) : 
     List[String] = {
     val listOfArr = fieldExtractor.getExtractedRecords()
    	         
     //list of fields to list of records
     val recs = listOfArr.asScala.map(a => {
       a.mkString(fieldDelimOut)
     })
     recs.toList
   }

   /**
   * @param fieldExtractor
   * @param fieldDelimOut
   * @return
   */
    def getNormalizedParentRecord(fieldExtractor : JsonFieldExtractor, 
     fieldDelimOut:String) : String = {
     val arr = fieldExtractor.getExtractedParentRecord()
     val rec = arr.mkString(fieldDelimOut)	
     rec
   }
    
   /**
   * @param fieldExtractor
   * @param fieldDelimOut
   * @return
   */
    def getNormalizedChildRecords(fieldExtractor : JsonFieldExtractor, 
     fieldDelimOut:String) : List[String] = {
     val childRecMap = fieldExtractor.getExtractedChildRecords()
     
     //records as list of strings
     val chMap = childRecMap.asScala.mapValues(v => {
         val recs = v.asScala.map(a => {
         a.mkString(fieldDelimOut)
       })
       recs
     })
    
     //consolidated flat list
     val allChRecs = chMap.flatMap(kv => {
       kv._2
     })
     allChRecs.toList
    }  
}