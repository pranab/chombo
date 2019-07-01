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


package org.chombo.spark.common

import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD
import org.chombo.util.SeasonalAnalyzer;
import org.chombo.util.BasicUtils
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

/**
 * Utility methods for seasonality
 * @author pranab
 *
 */
trait SeasonalUtility {
  
	/**
	 * @param jobConfig
	 * @param appConfig
	 * @param sType
	 * @param timeZoneShiftHours
	 * @param timeStampInMili
	 * @return
	 */
	def createSeasonalAnalyzer(jobConfig : JobConfiguration, appConfig: com.typesafe.config.Config, 
	    sType : String, timeZoneShiftHours:Int, timeStampInMili:Boolean) : SeasonalAnalyzer = {
		val seasonalAnalyzer = new SeasonalAnalyzer(sType)
		
		//additional configuration
    	if (seasonalAnalyzer.isHourRange()) {
    		val hourRangeStr = jobConfig.getMandatoryStringParam(appConfig, "seasonal.hourGroups", "missinfg hour rangen")
    		val hourRanges = BasicUtils.integerIntegerMapFromString(hourRangeStr, BasicUtils.DEF_FIELD_DELIM, 
    				BasicUtils.DEF_SUB_FIELD_DELIM, true)
    		seasonalAnalyzer.setHourRanges(hourRanges)
    	} else if (seasonalAnalyzer.isAnyDay()) {
    	  val days = jobConfig.getMandatoryStringListParam(appConfig, "specific.days", "missing days list").asScala.toArray
    	  val dateFormatStr = jobConfig.getMandatoryStringParam(appConfig, "date.formatStr", "missinfg date format string")
    	  val timeZone = jobConfig.getOptionalStringParam(appConfig, "time.zone") match {
    	    case Some(tz : String) => tz
    	    case None => null
    	  }
    	  val anyDays = BasicUtils.epochTimeIntegerMapFromString(days, BasicUtils.DEF_SUB_FIELD_DELIM, dateFormatStr, timeZone, false)
    	  seasonalAnalyzer.setAnyDays(anyDays)
    	} else if (seasonalAnalyzer.isAnyTimeRange()){
    		val timeRangeStr = jobConfig.getMandatoryStringParam(appConfig, "seasonal.timeRanges", "missinfg time ranges")
    		val timeRanges = BasicUtils.getIntPairList(timeRangeStr, BasicUtils.DEF_FIELD_DELIM, BasicUtils.DEF_SUB_FIELD_DELIM)
    		seasonalAnalyzer.setTimeRanges(timeRanges)
		} else if (seasonalAnalyzer.isWithHoliday()){
    	  val dateFormatStr = jobConfig.getMandatoryStringParam(appConfig, "date.format", "missinfg date format")
    	  val holidays = jobConfig.getMandatoryStringListParam(appConfig, "specific.days", "missing days list").asScala.toArray
    	  seasonalAnalyzer.withDateFormat(dateFormatStr).withDates(holidays)
		} else if (seasonalAnalyzer.isDayRangeOfWeek()){
    		val dayRangeStr = jobConfig.getMandatoryStringParam(appConfig, "seasonal.dayGroups", "missinfg day ranges")
    		val dayRanges = BasicUtils.integerIntegerMapFromString(dayRangeStr, BasicUtils.DEF_FIELD_DELIM, 
    				BasicUtils.DEF_SUB_FIELD_DELIM, true)
    		seasonalAnalyzer.setDayOfWeekRanges(dayRanges)
		} else if (seasonalAnalyzer.isMonthRangeOfYear()){
    		val monthRangeStr = jobConfig.getMandatoryStringParam(appConfig, "seasonal.monthGroups", "missinfg month ranges")
    		val monthRanges = BasicUtils.integerIntegerMapFromString(monthRangeStr, BasicUtils.DEF_FIELD_DELIM, 
    				BasicUtils.DEF_SUB_FIELD_DELIM, true)
    		seasonalAnalyzer.setMonthOfYearRanges(monthRanges)
		}
    	
    	if (timeZoneShiftHours > 0) {
    		seasonalAnalyzer.setTimeZoneShiftHours(timeZoneShiftHours)
    	}
    	seasonalAnalyzer.setTimeStampInMili(timeStampInMili)
        seasonalAnalyzer
	}
	
	/**
	 * @param jobConfig
	 * @param appConfig
	 * @param seasonalAnalysis
	 * @return
	 */
	def creatSeasonalAnalyzerMap(jobConfig : JobConfiguration, appConfig: com.typesafe.config.Config, 
	    seasonalAnalysis:Boolean, seasonalTypeInData:Boolean) :
		(Map[String, SeasonalAnalyzer], Int, Boolean) = {
	   val analyzerMap = scala.collection.mutable.Map[String, SeasonalAnalyzer]()
	   var timeStampFieldOrdinal = -1
	   var  timeStampInMili = true
	   if (seasonalAnalysis && seasonalTypeInData) {
		   	val seasonalCycleTypes = jobConfig.getMandatoryStringListParam(appConfig, "seasonal.cycleType", 
	        "missing seasonal cycle type").asScala.toArray
	        val timeZoneShiftHours = jobConfig.getIntParamOrElse(appConfig, "time.zoneShiftHours", 0)
	        timeStampFieldOrdinal = jobConfig.getMandatoryIntParam(appConfig, "time.fieldOrdinal", 
	        "missing time stamp field ordinal")
	        val timeStampInMili = jobConfig.getBooleanParamOrElse(appConfig, "time.inMili", true)
	        seasonalCycleTypes.foreach(sType => {
	        	val seasonalAnalyzer = createSeasonalAnalyzer(jobConfig, appConfig, sType, timeZoneShiftHours, timeStampInMili)
	        	analyzerMap += (sType -> seasonalAnalyzer)
	        })
	   } 
	   (analyzerMap.toMap, timeStampFieldOrdinal, timeStampInMili)
	}
	
	/**
	 * @param jobConfig
	 * @param appConfig
	 * @param seasonalAnalysis
	 * @return
	 */
	def creatSeasonalAnalyzerArray(jobConfig : JobConfiguration, appConfig: com.typesafe.config.Config, 
	    seasonalAnalysis:Boolean, seasonalTypeInData:Boolean) :
	   (Array[SeasonalAnalyzer],Int) = {
	   var analyzers = Array[SeasonalAnalyzer]()
	   val seasonalAnalyzers = if (seasonalAnalysis && !seasonalTypeInData) {
		   	val seasonalCycleTypes = jobConfig.getMandatoryStringListParam(appConfig, "seasonal.cycleType", 
	        "missing seasonal cycle type").asScala.toArray
	        val timeZoneShiftHours = jobConfig.getIntParamOrElse(appConfig, "time.zoneShiftHours", 0)
	        val timeStampFieldOrdinal = jobConfig.getMandatoryIntParam(appConfig, "time.fieldOrdinal", 
	        "missing time stamp field ordinal")
	        val timeStampInMili = jobConfig.getBooleanParamOrElse(appConfig, "time.inMili", true)
	        
	        analyzers = seasonalCycleTypes.map(sType => {
	        	val seasonalAnalyzer = createSeasonalAnalyzer(jobConfig, appConfig, sType, timeZoneShiftHours, timeStampInMili)
	        	seasonalAnalyzer
	        })
	        (analyzers, timeStampFieldOrdinal)
	   } else {
		   	(analyzers, 0)
	   }
	   seasonalAnalyzers
	}
	
	/**
	 * @param keyedRecs
	 * @param seasonalAnalysis
	 * @param idLen
	 * @return
	 */
	def filtInvalidSeasonalIndex(keyedRecs:RDD[(Record, Record)], seasonalAnalysis: Boolean, idLen:Int) : 
		RDD[(Record, Record)] = {
	  val filtKeyedRecs = 
	  if (seasonalAnalysis) {
	    val filt = keyedRecs.filter(v => {
	      val key = v._1
	      val ci = idLen + 1
	      key.getInt(ci) >= 0
	    })
	    filt
	  } else {
	    keyedRecs
	  }
	  filtKeyedRecs
	}
	
	/**
	 * @param items
	 * @param keyFieldOrdinals
	 * @param key
	 */
	def addPrimarykeys(items:Array[String], keyFieldOrdinals: Option[Array[Int]], key:Record)  {
	   keyFieldOrdinals match {
           case Some(fields : Array[Int]) => {
             for (kf <- fields) {
               key.addString(items(kf))
             }
           }
           case None =>
       }
	}
	
	/**
	 * @param seasonalAnalyzers
	 * @param items
	 * @param key
	 */
	def addSeasonalKeys(seasonalAnalyzers:Option[(Array[org.chombo.util.SeasonalAnalyzer], Int)], items:Array[String], 
	    key:Record) {
	     seasonalAnalyzers match {
	       case Some(seAnalyzers : (Array[SeasonalAnalyzer], Int)) => {
	         val timeStamp = items(seAnalyzers._2).toLong
	         val cIndex = SeasonalAnalyzer.getCycleIndex(seAnalyzers._1, timeStamp)
	         key.addString(cIndex.getLeft())
	         key.addInt(cIndex.getRight())
	       }
	       case None => 
	     }	  
 	}
	
	/**
	 * @param jobConfig
	 * @param appConfig
	 * @param analyzerMap
	 * @param analyzers
	 * @param items
	 * @param key
	 */
	def addSeasonalKeys(jobConfig:JobConfiguration, appConfig: com.typesafe.config.Config,
	    analyzerMap:(Map[String, SeasonalAnalyzer], Int, Boolean), 
	    analyzers:(Array[SeasonalAnalyzer],Int), items:Array[String], seasonal:Boolean, key:Record) {
		if (seasonal) {
		   val seasonalTypeFldOrd = jobConfig.getOptionalIntParam(appConfig, "seasonal.typeFldOrd")
		   seasonalTypeFldOrd match {
		     //seasonal type field in data
		     case Some(seasonalOrd:Int) => {
		       val seasonalType = items(seasonalOrd)
		       val analyzer = analyzerMap._1.get(seasonalType)
		       analyzer match {
		         case Some(an:SeasonalAnalyzer) => {
		            val analyzer = an
		        	val tsFldOrd = analyzerMap._2
		            val timeStamp = items(tsFldOrd).toLong
		            val cIndex = SeasonalAnalyzer.getCycleIndex(analyzer, timeStamp)
		            key.addString(cIndex.getLeft())
		            key.addInt(cIndex.getRight())
		         }
		         //unexpected
		         case None => throw new IllegalStateException("missing seasonal analyzer")
		       }
		     }
		     
		     //seasonal type in configuration
		     case None => {
		       if (analyzers._1.length > 0) {
		    	 val tsFldOrd = analyzers._2
		         val timeStamp = items(tsFldOrd).toLong
		         val cIndex = SeasonalAnalyzer.getCycleIndex(analyzers._1, timeStamp)
		         key.addString(cIndex.getLeft())
		         key.addInt(cIndex.getRight())
		       } else {
		         throw new IllegalStateException("missing seasonal analyzer raray")
		       }
		     }
		 }
	  }
	}
	
	
	/**
	 * @param keyFieldOrdinals
	 * @param seasonalAnalysis
	 * @return
	 */
	def getKeyLength(keyFieldOrdinals: Option[Array[Int]], seasonalAnalysis:Boolean) : Int = {
	     var keyLen = 0
	     keyFieldOrdinals match {
	     	case Some(fields : Array[Int]) => keyLen +=  fields.length
	     	case None =>
	     }
	     keyLen += (if (seasonalAnalysis) 2 else 0)
	     keyLen
	}
	
	/**
	 * @param key
	 * @param seasonal
	 * @param globalModel
	 * @return
	 */
	def getModelKey(key:Record, seasonal:Boolean, globalModel:Boolean): Record = {
	  if (globalModel) {
	    val keyLen = 1 + (if (seasonal) 2 else 0) + 1
 	    val mKey = Record(keyLen)
 	    mKey.addString("all")
 	    if (seasonal) {
 	    	mKey.addString(key.getString(-2))
 	    	mKey.addInt(key.getInt(-1))
 	    }
 	    mKey
	  } else {
	    Record(key)
	  }
	}
}