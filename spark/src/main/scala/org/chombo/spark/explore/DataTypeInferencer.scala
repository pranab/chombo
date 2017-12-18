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
	   val ssnPattern = getPattern(appConfig, "verify.ssn" : String, BaseAttribute.PATTERN_STR_SSN)
	   val phoneNumPattern = getPattern(appConfig, "verify.phoneNum" : String, BaseAttribute.PATTERN_STR_PHONE_NUM)
	   val maxAge = this.getOptionalIntParam(appConfig, "max.age")
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   val numTypes = 9
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //key by attribute index
	   val typeCounts = data.flatMap(line => {
	     val items  =  line.split(fieldDelimIn, -1) 
	     val size = items.length
	     
	     attrList.map(attr => {
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
	           
	           //ssn
	           val isSsn = isMatched(value, ssnPattern)
	           count = if (isSsn) 1 else 0
	           countRec.add(BaseAttribute.DATA_TYPE_SSN, count)
	           
	           //phone number
	           val isPhoneNum = isMatched(value, phoneNumPattern)
	           count = if (isPhoneNum) 1 else 0
	           countRec.add(BaseAttribute.DATA_TYPE_PHONE_NUM, count)
	           
	           //string
	           countRec.add(BaseAttribute.DATA_TYPE_STRING, 1)

	         }
	       }
	       
	       //any type
	       countRec.add(BaseAttribute.DATA_TYPE_ANY, 1)
	       
	     })
	     
	     
	     
	     
	   })
	   
   }
   
   
   /**
    * @param master
    * @param appName
    * @param executorMemory
    * @return
    */
   def getPattern(appConfig : Config, paramName : String, patternStr : String) : Option[java.util.regex.Pattern] = {
	   val verifyFlag = this.getBooleanParamOrElse(appConfig, paramName, true)
	   val pattern = verifyFlag match {
	     case true => {
	       Some(Pattern.compile(patternStr))
	     }
	     case false => None
	   }
       pattern
   }
   
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
   
}