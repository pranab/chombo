/*
 * chombo: Hadoop Map Reduce utility
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

package org.chombo.util;

import java.util.Map;

/**
 * @author pranab
 *
 */
public class SeasonalAnalyzer {
    private long parentCycleIndex;
    private int cycleIndex;
    private String seasonalCycleType;
    private Map<Integer, Integer> hourRanges;
    private boolean timeStampInMili;
    private long timeZoneShiftSec;
    public  static final String QUARTER_HOUR_OF_DAY = "quarterHourOfDay";
    public static final String  QUARTER_HOUR_OF_WEEK_DAY = "quarterHourOfWeekDay";
    public static final String  QUARTER_HOUR_OF_WEEK_END_DAY = "quarterHourOfWeekEndDay";
    public static final String  HALF_HOUR_OF_DAY = "halfHourOfDay";
    public static final String  HALF_HOUR_OF_WEEK_DAY = "halfHourOfWeekDay";
    public static final String  HALF_HOUR_OF_WEEK_END_DAY = "halfHourOfWeekEndDay";
    public static final String  HOUR_OF_DAY = "hourOfDay";
    public static final String  HOUR_OF_WEEK_DAY = "hourOfWeekDay";
    public static final String  HOUR_OF_WEEK_END_DAY = "hourOfWeekEndDay";
    public static final String  DAY_OF_WEEK  = "dayOfWeek";
    public static final String  WEEK_DAY_OF_WEEK  = "weekDayOfWeek";
    public static final String  WEEK_END_DAY_OF_WEEK  = "weekEndDayOfWeek";
    public static final String  HOUR_RANGE_OF_WEEK_DAY  = "hourRangeOfWeekDay";
    public static final String  HOUR_RANGE_OF_WEEK_END_DAY  = "hourRangeOfWeekEndDay";
    public static final String  MONTH_OF_YEAR = "monthOfYear";
    
    private static long secInWeek =7L * 24 * 60 * 60;
    private static long secInDay =24L * 60 * 60;
    private static long secInHour = 60L * 60;
    private static long secInHalfHour = 30L * 60;
    private static long secInQuarterHour = 15L * 60;
    private static long secInYear = secInDay * 365;
    private static int[] daysInMonth = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    
    /**
     * @param seasonalCycleType
     */
    public SeasonalAnalyzer(String seasonalCycleType) {
		super();
		this.seasonalCycleType = seasonalCycleType;
	}

	/**
	 * @param seasonalCycleType
	 * @param hourRanges
	 */
	public SeasonalAnalyzer(String seasonalCycleType,
			Map<Integer, Integer> hourRanges) {
		super();
		this.seasonalCycleType = seasonalCycleType;
		this.hourRanges = hourRanges;
	}

	/**
     * Calculates cycle index and parent cycle index
     * @param timeStamp
     */
    public int  getCycleIndex(long timeStamp) {
    	//convert to sec and adjust for time stamp
    	if (timeStampInMili) {
    		timeStamp /= 1000;
    	}
    	timeStamp += timeZoneShiftSec;
    	
    	long  weekDayIndex = 0;
    	if (seasonalCycleType.equals(DAY_OF_WEEK)) {
        	parentCycleIndex = timeStamp / secInWeek;
    		cycleIndex = (int)((timeStamp % secInWeek) / secInDay);
    	} else if (seasonalCycleType.equals(WEEK_DAY_OF_WEEK)) {
        	parentCycleIndex = timeStamp / secInWeek;
    		cycleIndex = (int)((timeStamp % secInWeek) / secInDay);
    		if (cycleIndex > 4) {
    			cycleIndex = -1;
    		}
    	} else if (seasonalCycleType.equals(WEEK_END_DAY_OF_WEEK)) {
        	parentCycleIndex = timeStamp / secInWeek;
    		cycleIndex = (int)((timeStamp % secInWeek) / secInDay);
    		if (cycleIndex < 5) {
    			cycleIndex = -1;
    		}
    	} else if (seasonalCycleType.equals(HOUR_OF_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
    		cycleIndex = (int)((timeStamp % secInDay) / secInHour);
    	}  else if (seasonalCycleType.equals(HOUR_OF_WEEK_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
           	weekDayIndex = parentCycleIndex % 7;
           	if (weekDayIndex < 5) {
        		cycleIndex = (int)((timeStamp % secInDay) / secInHour);
         	} else {
         		cycleIndex = -1;
         	}
    	}  else if (seasonalCycleType.equals(HOUR_OF_WEEK_END_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
            if (weekDayIndex > 4) {
        		cycleIndex = (int)((timeStamp % secInDay) / secInHour);
         	} else {
         		cycleIndex = -1;
         	}
    	} else  if (seasonalCycleType.equals(HALF_HOUR_OF_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
    		cycleIndex = (int)((timeStamp % secInDay) / secInHalfHour);
    	} else  if (seasonalCycleType.equals(HALF_HOUR_OF_WEEK_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
        	weekDayIndex = parentCycleIndex % 7;
        	if (weekDayIndex < 5) {
        		cycleIndex = (int)((timeStamp % secInDay) / secInHalfHour);
        	} else {
        		cycleIndex = -1;
        	}
    	} else  if (seasonalCycleType.equals(HALF_HOUR_OF_WEEK_END_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
        	weekDayIndex = parentCycleIndex % 7;
        	if (weekDayIndex > 4) {
        		cycleIndex = (int)((timeStamp % secInDay) / secInHalfHour);
        	} else {
        		cycleIndex = -1;
        	}
    	} else  if (seasonalCycleType.equals(QUARTER_HOUR_OF_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
    		cycleIndex = (int)((timeStamp % secInDay) / secInQuarterHour);
    	} else  if (seasonalCycleType.equals(QUARTER_HOUR_OF_WEEK_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
        	weekDayIndex = parentCycleIndex % 7;
        	if (weekDayIndex < 5) {
        		cycleIndex = (int)((timeStamp % secInDay) / secInQuarterHour);
        	} else {
        		cycleIndex = -1;
        	}
    	}  else  if (seasonalCycleType.equals(QUARTER_HOUR_OF_WEEK_END_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
        	weekDayIndex = parentCycleIndex % 7;
        	if (weekDayIndex > 4) {
        		cycleIndex = (int)((timeStamp % secInDay) / secInQuarterHour);
        	} else {
        		cycleIndex = -1;
        	}
    	}  else  if (seasonalCycleType.equals(HOUR_RANGE_OF_WEEK_DAY)) {
    		parentCycleIndex = timeStamp / secInDay;
    		weekDayIndex = parentCycleIndex % 7;
    		if (weekDayIndex < 5) {
    			int hourCycleIndex = (int)((timeStamp % secInDay) / secInHour);
    			Integer hourGroup = hourRanges.get(hourCycleIndex);
    			cycleIndex = hourGroup != null ? hourGroup : -1;
    		} else {
    			cycleIndex = -1;
    		}
    	}  else  if (seasonalCycleType.equals(HOUR_RANGE_OF_WEEK_END_DAY)) {
    		parentCycleIndex = timeStamp / secInDay;
    		weekDayIndex = parentCycleIndex % 7;
    		if (weekDayIndex >= 5) {
    			int hourCycleIndex = (int)((timeStamp % secInDay) / secInHour);
    			Integer hourGroup = hourRanges.get(hourCycleIndex);
    			cycleIndex = hourGroup != null ? hourGroup : -1;
    		} else {
    			cycleIndex = -1;
    		}
    	}  else  if (seasonalCycleType.equals(MONTH_OF_YEAR)) {
    		monthOfYearCycleIndex(timeStamp);
    	} else {
    		throw new IllegalArgumentException("invalid cycle type");
    	}
    	
    	return cycleIndex;
    }

	public void setHourRanges(Map<Integer, Integer> hourRanges) {
		this.hourRanges = hourRanges;
	}

	public void setTimeStampInMili(boolean timeStampInMili) {
		this.timeStampInMili = timeStampInMili;
	}

	public void setTimeZoneShiftHours(long timeZoneShiftHours) {
		this.timeZoneShiftSec = timeZoneShiftHours * secInHour ;
	}

	public long getParentCycleIndex() {
		return parentCycleIndex;
	}
    
	private  void  monthOfYearCycleIndex(long timeStamp) {
		long secToYear = 0;
		int year = 1971;
		for (; secToYear < timeStamp; ++year) {
			secToYear += secInYear;
			if (year % 4 == 0) {
				secToYear += secInDay;
			}
		}
		
		long remSec =  timeStamp - secToYear;
		daysInMonth[1] = year % 4 == 0 ? 29 : 28; 
		long secIntoYear = 0;
		for (int i = 0; i < 12; ++i) {
			secIntoYear += daysInMonth[i] + secInDay;
			if (secIntoYear > remSec) {
				cycleIndex = i - 1;
				break;
			}
		}
	}
	
}
