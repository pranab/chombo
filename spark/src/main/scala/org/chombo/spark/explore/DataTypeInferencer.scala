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

import scala.util.control.Breaks._
import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import com.typesafe.config.Config
import org.chombo.spark.common.Record
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import org.chombo.util.BaseAttribute

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
	       val recSize = getMandatoryIntParam(appConfig, "rec.size", "missing recor size parameter")
	       val attrs = (0 to recSize-1).toList
	       attrs
	     }
	   }
	   
	   val verifyDate = getBooleanParamOrElse(appConfig, "verify.date", true)
	   val timeWindowYears = getOptionalIntParam(appConfig, "time.window.years")
	   val timeWindowBegin = timeWindowYears match {
	     case Some(windowYears : Int) => {
	       val now = System.currentTimeMillis();
           val windowBegin = now - windowYears * BasicUtils.MILISEC_PER_DAY * 365
           Some(windowBegin)
	     }
	     case None => None
	   }
	   
	   val dateFormatStrList = getOptionalStringListParam(appConfig, "date.format.str.list")
	   val dateFormaList = dateFormatStrList match {
	     case Some(formatStrList : java.util.List[String]) => {
	       val dtFormat = formatStrList.asScala.toList.map(str => new SimpleDateFormat(str))
	       Some(dtFormat)
	     }
	     case None => None
	   }
	   
	   if (verifyDate && timeWindowYears == None && dateFormatStrList == None) {
	     throw new IllegalStateException("eithet date format list or time window must be provided for date verification : ")
	   }
	   
	   val ssnPattern = getPattern(appConfig, "verify.ssn" : String, BaseAttribute.PATTERN_STR_SSN)
	   val phoneNumPattern = getPattern(appConfig, "verify.phone.num" : String, BaseAttribute.PATTERN_STR_PHONE_NUM)
	   val streetAddressPattern = getPattern(appConfig, "verify.street.address" : String, BaseAttribute.PATTERN_STR_STREET_ADDRESS)
	   val cityPattern = getPattern(appConfig, "verify.city" : String, BaseAttribute.PATTERN_STR_CITY)
	   val zipPattern = getPattern(appConfig, "verify.zip" : String, BaseAttribute.PATTERN_STR_ZIP)
	   val currencyPattern = getPattern(appConfig, "verify.currency" : String, BaseAttribute.PATTERN_STR_CURRENCY)
	   val maxAge = getOptionalIntParam(appConfig, "max.age")
	   val ambiguityThresholdPercent = getIntParamOrElse(appConfig, "ambiguity.threshold.percent", 90)
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   //number of data types
	   val numTypes = 12
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //attribute index for key and count for different type as value
	   val typeCounts = data.flatMap(line => {
	     val items  =  line.split(fieldDelimIn, -1) 
	     val size = items.length
	     
	     val attrTypeCount = attrList.map(attr => {
	       val countRec = Record(2 * numTypes)
	       val value = items(attr)
	       
	       //epoch time
	       var count = timeWindowBegin match {
	         case Some(windowBegin : Long) => {
	           val isEpoch = windowBegin > 0 && BasicUtils.isLong(value) && value.toLong > windowBegin
	           if (isEpoch) 1 else 0
	         }
	         case None => 0
	       }
	       countRec.add(BaseAttribute.DATA_TYPE_EPOCH_TIME, count)
	       
	       //int
	       val isInt = BasicUtils.isInt(value)
	       count = if (isInt) 1 else  0
	       countRec.add(BaseAttribute.DATA_TYPE_INT, count)
	       var isNumeric = isInt
	       
	       //age
	       count = maxAge match {
	         case Some(max : Int) => {
	           val isAge = isInt && value.toInt < max 
	           if (isAge) 1 else 0
	         }
	         case None => 0
	       }
	       countRec.add(BaseAttribute.DATA_TYPE_AGE, count)
	       
	       //float
	       val isFloat = BasicUtils.isFloat(value)
	       count = if (isFloat) 1 else  0
	       countRec.add(BaseAttribute.DATA_TYPE_FLOAT, count)
	       isNumeric = isFloat
	       
	       isNumeric match {
	         case true => {
	           //already found numeric
	           countRec.add(BaseAttribute.DATA_TYPE_DATE, 0)
	           countRec.add(BaseAttribute.DATA_TYPE_SSN, 0)
	           countRec.add(BaseAttribute.DATA_TYPE_PHONE_NUM, 0)
	           countRec.add(BaseAttribute.DATA_TYPE_STRING, 0)
	         }
	         case false => {
	           //date
	           val isDate = dateFormaList match {
	             case Some(dateFormats : List[SimpleDateFormat]) => {
	               var isDate = false
	               dateFormats.foreach(format => {
	                 isDate = BasicUtils.isDate(value, format)
		    		 if (isDate)
		    			break;
	               })
	               isDate
	             }
	             case None => false 
	           }
	           count = if (isDate) 1 else 0
	           countRec.add(BaseAttribute.DATA_TYPE_DATE, count)
	           
	           //currency
	           val isCurrency = isMatched(value, currencyPattern)
	           countRec.add(BaseAttribute.DATA_TYPE_CURRENCY, if (isCurrency) 1 else 0)

	           //ssn
	           val isSsn = isMatched(value, ssnPattern)
	           countRec.add(BaseAttribute.DATA_TYPE_SSN, if (isSsn) 1 else 0)
	           
	           //phone number
	           val isPhoneNum = isMatched(value, phoneNumPattern)
	           countRec.add(BaseAttribute.DATA_TYPE_PHONE_NUM, if (isPhoneNum) 1 else 0)
	           
	           //zip
	           val isZip = isMatched(value, zipPattern)
	           countRec.add(BaseAttribute.DATA_TYPE_ZIP, if (isZip) 1 else 0)

	           //street address
	           val isStreetAddr = if (!isZip) isMatched(value, streetAddressPattern) else false
	           countRec.add(BaseAttribute.DATA_TYPE_STREET_ADDRESS, if (isStreetAddr) 1 else 0)
	           
	           //city
	           val isCity = if (!isZip && !isStreetAddr) isMatched(value, cityPattern) else false
	           countRec.add(BaseAttribute.DATA_TYPE_CITY, if (isCity) 1 else 0)

	           //string
	           countRec.add(BaseAttribute.DATA_TYPE_STRING, 1)

	         }
	       }
	       
	       //any type
	       countRec.add(BaseAttribute.DATA_TYPE_ANY, 1)
	       (attr.toInt, countRec)
	     })
	     attrTypeCount
	   })
	   
	   //aggregate counts for each type
	   val aggrTypeCounts = typeCounts.reduceByKey((v1,v2) => {
	     val countRec = Record(2 * numTypes)
	     var offset = 0;
	     for (i <- 0 to numTypes-1) {
	       val dataType = v1.getString(offset)
	       offset += 1
	       val typeCount = v1.getInt(offset) + v2.getInt(offset)
	       countRec.add(dataType, typeCount)
	       offset += 1
	     }
	     countRec
	   })
	   
	   //infer types
	   val inferredTypes = aggrTypeCounts.mapValues(r => {
	     var dataType = BaseAttribute.DATA_TYPE_STRING
	     val typeCounts = scala.collection.mutable.Map[String, Int]()
	     var offset = 0;
	     for (i <- 0 to numTypes-1) {
	       val dataType = r.getString(offset)
	       offset += 1
	       val typeCount = r.getInt(offset)
	       offset += 1
	       typeCounts += (dataType -> typeCount)
	     }

	     val anyCount = typeCounts(BaseAttribute.DATA_TYPE_ANY)
	     val intCount = typeCounts(BaseAttribute.DATA_TYPE_INT)
	     val floatCount = typeCounts(BaseAttribute.DATA_TYPE_FLOAT)
	     val ambiguityThreshold = (anyCount * ambiguityThresholdPercent) / 100
	     var result = (false, false, 0.0)
	     if (intCount == anyCount) {
	       //int based
	       val numericTypes = Array(BaseAttribute.DATA_TYPE_EPOCH_TIME, BaseAttribute.DATA_TYPE_AGE)
	       numericTypes.foreach(numType => {
	         result = discoverType(numType,  typeCounts, anyCount, ambiguityThreshold)
	         if (result._1) {
	           dataType = numType 
	           break
	         }
	       }) 
	       
	       //int
	       if (!result._1) dataType = BaseAttribute.DATA_TYPE_INT
	     } else if (floatCount == anyCount) {
	    	 //float
	         dataType = BaseAttribute.DATA_TYPE_FLOAT
	     } else {
	       //string based
	       val stringTypes = Array(
	           BaseAttribute.DATA_TYPE_CURRENCY,
	           BaseAttribute.DATA_TYPE_DATE, BaseAttribute.DATA_TYPE_SSN, 
	           BaseAttribute.DATA_TYPE_PHONE_NUM, BaseAttribute.DATA_TYPE_STREET_ADDRESS, 
	           BaseAttribute.DATA_TYPE_CITY, BaseAttribute.DATA_TYPE_ZIP)
	       stringTypes.foreach(strType => {
	         result = discoverType(strType,  typeCounts, anyCount, ambiguityThreshold)
	         if (result._1) {
	           dataType = strType 
	           break
	         }
	       }) 
	     }
	     val info = if (result._2) " (ambiguous with correctness probability " + BasicUtils.formatDouble(result._3) + " )" else ""
	     dataType + info
	   })
	   
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
   
}