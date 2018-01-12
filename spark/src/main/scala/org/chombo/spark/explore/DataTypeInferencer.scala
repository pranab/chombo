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
import org.chombo.util.BasicUtils
import com.typesafe.config.Config
import org.chombo.spark.common.Record
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import org.chombo.util.BaseAttribute
import org.chombo.types.DataTypeHandler

/**
 * @param args
 * @return
*/
object DataTypeInferencer extends JobConfiguration  {
  /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "dataTypeInferencer"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val attributes = getOptionalIntListParam(appConfig,"attributes")
	   val attrList = attributes match {
	     case Some(attr : java.util.List[Integer]) => {
	       attr.asScala.toList.map {_.toInt}
	     }
	     case None => {
	       val recSize = getMandatoryIntParam(appConfig, "rec.size", "missing record size parameter")
	       val attrs = (0 to recSize-1).toList
	       attrs
	     }
	   }
	   
	   //string types
	   val stringTypeHandler  = new DataTypeHandler()
	   val stringDataTypes = new java.util.HashSet[String]()
	   if (getBooleanParamOrElse(appConfig, "verify.ssn", true)) stringDataTypes.add(BaseAttribute.DATA_TYPE_SSN)
	   if (getBooleanParamOrElse(appConfig, "verify.phoneNum", true)) stringDataTypes.add(BaseAttribute.DATA_TYPE_PHONE_NUM)
	   if (getBooleanParamOrElse(appConfig, "verify.streetAddress", true)) stringDataTypes.add(BaseAttribute.DATA_TYPE_STREET_ADDRESS)
	   if (getBooleanParamOrElse(appConfig, "verify.city", true)) stringDataTypes.add(BaseAttribute.DATA_TYPE_CITY)
	   if (getBooleanParamOrElse(appConfig, "verify.zip", true)) stringDataTypes.add(BaseAttribute.DATA_TYPE_ZIP)
	   if (getBooleanParamOrElse(appConfig, "verify.currency", true)) stringDataTypes.add(BaseAttribute.DATA_TYPE_CURRENCY)
	   if (getBooleanParamOrElse(appConfig, "verify.monetaryAmount", true)) stringDataTypes.add(BaseAttribute.DATA_TYPE_MONETARY_AMOUNT)
	   stringTypeHandler.addStringDataTypes(stringDataTypes)
	   
	   //id types
	   if (getBooleanParamOrElse(appConfig, "verify.id", true)) {
		   val idLengths = getMandatoryIntListParam(appConfig, "idLengths", "missing ID filed lenghths")
		   stringTypeHandler.addIdType(idLengths)
	   }
	   
	   //numeric types
	   val numericTypeHandler  = new DataTypeHandler()
	   numericTypeHandler.addNumericTypes
	   
	   //epoch time
	   val verifyDate = getBooleanParamOrElse(appConfig, "verify.date", true)
	   val timeWindowYears = getOptionalIntParam(appConfig, "time.window.years")
	   timeWindowYears match {
	     case Some(windowYears : Int) => {
	       val now = System.currentTimeMillis();
           val windowBegin = now - windowYears * BasicUtils.MILISEC_PER_DAY * 365
           numericTypeHandler.addEpochTimeType(windowBegin, now, 80)
	     }
	     case None => None
	   }
	   
	   //date
	   val dateFormatStrList = getOptionalStringListParam(appConfig, "date.format.str.list")
	   dateFormatStrList match {
	     case Some(formatStrList : java.util.List[String]) => {
	       stringTypeHandler.addDateType(formatStrList)
	     }
	     case None => None
	   }
	   
	   if (verifyDate && timeWindowYears == None && dateFormatStrList == None) {
	     throw new IllegalStateException("eithet date format list or time window must be provided for date verification : ")
	   }
	   
	   //age type
	   if (getBooleanParamOrElse(appConfig, "verify.age", true)) {
		   val maxAge = getMandatoryIntParam(appConfig, "max.age", "missing max age")
		   numericTypeHandler.addAgeType(0, maxAge, 90)
	   }
	   
	   //custom types
	   setCustomStringTypes(appConfig, stringTypeHandler)
	   setCustomIntTypes(appConfig, numericTypeHandler)
	   
	   //consolidate type name list
	   stringTypeHandler.mergeTypeLists(numericTypeHandler)
	   
	   val ambiguityThresholdPercent = getIntParamOrElse(appConfig, "ambiguity.threshold.percent", 90)
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   //number of data types
	   val allDataTypes = stringTypeHandler.getAllDataTypes()
	   val numTypes = allDataTypes.length
	   val countRecIndex = scala.collection.mutable.Map[String, Int]()
	   allDataTypes.zipWithIndex.foreach(v => {
	     val index = 2 * v._2 + 1
	     countRecIndex += (v._1 ->  index)
	   })
	   
	   numericTypeHandler.prepare()
	   stringTypeHandler.prepare()
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //attribute index for key and count for different type as value
	   val typeCounts = data.flatMap(line => {
	     val items  =  line.split(fieldDelimIn, -1) 
	     val size = items.length
	     
	     val attrTypeCount = attrList.map(attr => {
	       val countRec = Record(2 * numTypes)
	       val value = items(attr)
	       initializeCount(countRec, allDataTypes, debugOn)
	       
	       val matchedNumericTypes = numericTypeHandler.findTypes(value).asScala.toList
	       //if (debugOn)
	       //  println("matched numeric types:" + matchedNumericTypes)
	       setMatchedTypeCount(countRec, matchedNumericTypes, countRecIndex, debugOn)
	       val isNumeric = matchedNumericTypes.size > 0
	       
	       val matchedStringTypes = stringTypeHandler.findTypes(value).asScala.toList
	       //if (debugOn)
	       //  println("matched string types:" + matchedStringTypes)
	       setMatchedTypeCount(countRec, matchedStringTypes, countRecIndex, debugOn)
	       
	       (attr.toInt, countRec)
	     })
	     
	     attrTypeCount
	   })
	   
	   //aggregate counts for each type
	   if (debugOn) 
	     println("aggregating counts")
	   val aggrTypeCounts = typeCounts.reduceByKey((v1,v2) => {
	     val countRec = Record(2 * numTypes)
	     for (i <- 0 to numTypes-1) {
	       val index = 2 * i
	       val dataType = v1.getString(index)
	       val typeCount = v1.getInt(index + 1) + v2.getInt(index + 1)
	       countRec.addString(dataType)
	       countRec.addInt(typeCount)
	       //if (debugOn) 
	       //  println("agregation type:" + dataType + " count:" + typeCount)
	     }
	     countRec
	   })
	   
	   //val aggrTypeCountsCol = aggrTypeCounts.collect
	   
	   //infer types
	   if (debugOn) 
	     println("inferring types")
	   val numericTypes = stringTypeHandler.getAllNumericDataTypes()
	   val stringTypes = stringTypeHandler.getAllStringDataTypes()
	   val inferredTypes = aggrTypeCounts.map(r => {
	     if (debugOn) 
	       println("attr ordinal:" + r._1 + " count rec: " + r._2)
	     var dataType = BaseAttribute.DATA_TYPE_STRING
	     val typeCounts = buidTypeCountMap(r._2, numTypes)

	     val anyCount = typeCounts(BaseAttribute.DATA_TYPE_ANY)
	     val intCount = typeCounts(BaseAttribute.DATA_TYPE_INT)
	     val floatCount = typeCounts(BaseAttribute.DATA_TYPE_FLOAT)
	     //if (debugOn)
	     //  println("anyCount: " + anyCount + " intCount:" + intCount + " floatCount:" + floatCount)
	       
	     val ambiguityThreshold = (anyCount * ambiguityThresholdPercent) / 100
	     var result = (false, false, 100.0)
	     if (intCount == anyCount) {
	       //int based
	       numericTypes.foreach(numType => {
	         if (!result._1) {
	           result = discoverType(numType,  typeCounts, anyCount, ambiguityThreshold)
	           if (result._1) {
	             dataType = numType 
	             if (debugOn) println("numeric inferred type:" + dataType)
	           }
	         }
	       }) 
	     } else if (floatCount == anyCount) {
	    	 //float
	         dataType = BaseAttribute.DATA_TYPE_FLOAT
	         result = (true, false, 100.0)
	         if (debugOn) println("float inferred type:" + dataType)
	     } 
	     
	     //string based
	     if (!result._1) {
	       stringTypes.foreach(strType => {
	         if (!result._1) {
	        	 result = discoverType(strType,  typeCounts, anyCount, ambiguityThreshold)
	        	 if (result._1) {
	               dataType = strType 
	               if (debugOn) println("string inferred type:" + dataType)
	             }
	         }
	       }) 
	     }
	     
	     //int
	     if (!result._1 && intCount == anyCount) {
	       dataType = BaseAttribute.DATA_TYPE_INT
	       if (debugOn) println("int inferred type:" + dataType)
	     }
	     
	     val info = 
	       if (result._2) 
	         " (ambiguous with correctness probability " + BasicUtils.formatDouble(result._3) + " )" 
	       else 
	         ""
	     val typeInfo = dataType + info
	     (r._1, typeInfo)
	   })
	   if (debugOn)
	     println("done")
	   
       if (debugOn) {
         val records = inferredTypes.collect
         records.foreach(r => println(r._1 + fieldDelimOut + r._2))
       }
	   
	   //output
	   if(saveOutput) {	   
	     inferredTypes.saveAsTextFile(outputPath) 
	   }
   }
   
    /**
     * @param config
     * @param paramName
     * @return
     */   
   def initializeCount(countRec : Record, allDataTypes : Array[String], debugOn : Boolean) {
     allDataTypes.foreach(t => {
       val count = if (t.equals(BaseAttribute.DATA_TYPE_ANY)) 1 else 0
       countRec.addString(t)
       countRec.addInt(count)
     })
   }
   
   /**
   * @param countRec
   * @param matchedTypes
   * @param countRecIndex
   */
   def setMatchedTypeCount(countRec : Record, matchedTypes : List[String], 
       countRecIndex : scala.collection.mutable.Map[String,Int], debugOn : Boolean) {
     matchedTypes.foreach(t => {
       val index = countRecIndex(t)
       //if (debugOn)
       //  println("setting type: " + t + " index: " + index)
       countRec.addInt(index, 1)
     })
   }
   
   /**
    * @param appConfig
 	* @param stringTypeHandler
 	*/
   def setCustomStringTypes(appConfig : Config, stringTypeHandler : DataTypeHandler) {
     val strTypes = getOptionalStringListParam(appConfig, "customStringTypes")
     strTypes match {
       case Some(stTypes : java.util.List[String]) => {
         stTypes.asScala.foreach(t => {
           val typeConfig  = appConfig.getConfig(t)
           val name = t
           val regex = getMandatoryStringParam(typeConfig, "regex", "missing custom type regex")
           val strength = getMandatoryIntParam(typeConfig, "strength", "missing custom type strength")
           val length = getOptionalIntParam(typeConfig, "length")
           length match {
             case Some(realLength : Int) => {
               
             }
             case None => {
               stringTypeHandler.addCustomStringType(name, regex, strength)
             }
           }
         })
       }
       case None =>
     }
   }

    /**
     * @param appConfig
     * @param numericTypeHandler
     */
    def setCustomIntTypes(appConfig : Config,  numericTypeHandler : DataTypeHandler) {
     val numTypes = getOptionalStringListParam(appConfig, "customIntTypes")
     numTypes match {
       case Some(nuTypes : java.util.List[String]) => {
         nuTypes.asScala.foreach(t => {
           val typeConfig  = appConfig.getConfig(t)
           val name = t
           val minVal = getMandatoryIntParam(typeConfig, "minVal", "missing custom type regex")
           val maxVal = getMandatoryIntParam(typeConfig, "maxVal", "missing custom type regex")
           val strength = getMandatoryIntParam(typeConfig, "strength", "missing custom type strength")
           numericTypeHandler.addCustomIntType( name, minVal, maxVal, strength)
         })
       }
       case None =>
     }
   }
  
   
   /**
    * @param master
    * @param appName
    * @param executorMemory
    * @return
    */
   def getPattern(appConfig : Config, paramName : String, patternStr : String) : Option[java.util.regex.Pattern] = {
	   val verifyFlag = getBooleanParamOrElse(appConfig, paramName, true)
	   val pattern = verifyFlag match {
	     case true => {
	       Some(Pattern.compile(patternStr))
	     }
	     case false => None
	   }
       pattern
   }
   
   /**
    * @param value
    * @param pattern
    * @return
    */
   def isMatched(value: String, pattern : Option[Pattern]) : Boolean = {
     val isMatched = pattern match {
	   case Some(patt : Pattern) => {
	     val matcher = patt.matcher(value)
	     matcher.matches()
	   }
	   case None => false
	 }
     isMatched
   }
   
   /**
    * @param dataType
    * @param typeCounts
    * @param anyCount
    * @param ambiguityThreshold
    * @return
    */
   def discoverType(dataType : String,  typeCounts : scala.collection.mutable.Map[String, Int], anyCount : Int,
       ambiguityThreshold : Int) : (Boolean, Boolean, Double) =  {
     val typeCount = typeCounts(dataType)
     var matched = false
     var isAmbiguous = false
     var discoveryProb = 100.0
     if (typeCount == anyCount) {
		matched = true
	 } else if (typeCount > ambiguityThreshold) {
		matched = true
		isAmbiguous = true;
		discoveryProb = (typeCount * 100.0) / anyCount;
	 } 
     (matched, isAmbiguous, discoveryProb)
   } 
   
    /**
     * @param countRec
     * @param numTypes
	 * @return
	 */  
     def buidTypeCountMap(countRec : Record, numTypes : Int) : scala.collection.mutable.Map[String, Int] = {
     val typeCounts = scala.collection.mutable.Map[String, Int]()
	 for (i <- 0 to numTypes-1) {
	   val index = 2 * i
	   val dataType = countRec.getString(index)
	   val typeCount = countRec.getInt(index + 1)
	   typeCounts += (dataType -> typeCount)
	 }
     typeCounts
   }
   
}