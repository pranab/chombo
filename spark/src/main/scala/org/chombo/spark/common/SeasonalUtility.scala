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
    	if (seasonalAnalyzer.isHourRange()) {
    		val hourRangeStr = jobConfig.getMandatoryStringParam(appConfig, "seasonal.hourGroups", "missinfg hour rangen")
    		val hourRanges = BasicUtils.integerIntegerMapFromString(hourRangeStr, BasicUtils.DEF_FIELD_DELIM, 
    				BasicUtils.DEF_SUB_FIELD_DELIM, true)
    		seasonalAnalyzer.setHourRanges(hourRanges)
    	}
    	if (seasonalAnalyzer.isAnyDay()) {
    	  val days = jobConfig.getMandatoryStringListParam(appConfig, "specific.days", "missing days list").asScala.toArray
    	  val dateFormatStr = jobConfig.getMandatoryStringParam(appConfig, "date.formatStr", "missinfg date format string")
    	  val timeZone = jobConfig.getOptionalStringParam(appConfig, "time.zone") match {
    	    case Some(tz : String) => tz
    	    case None => null
    	  }
    	  val anyDays = BasicUtils.epochTimeIntegerMapFromString(days, BasicUtils.DEF_SUB_FIELD_DELIM, dateFormatStr, timeZone, false)
    	  seasonalAnalyzer.setAnyDays(anyDays)
    	}
    	
    	if (timeZoneShiftHours > 0) {
    		seasonalAnalyzer.setTimeZoneShiftHours(timeZoneShiftHours)
    	}
    	seasonalAnalyzer.setTimeStampInMili(timeStampInMili)
        seasonalAnalyzer
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
	
}