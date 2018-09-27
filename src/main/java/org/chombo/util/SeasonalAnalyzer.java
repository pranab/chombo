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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author pranab
 *
 */
public class SeasonalAnalyzer implements Serializable {
    private long parentCycleIndex;
    private int cycleIndex;
    private String seasonalCycleType;
    private Map<Integer, Integer> hourRanges;
    private boolean timeStampInMili;
    private long timeZoneShiftSec;
    private List<Pair<Integer, Integer>> timeRanges;
    private long secToYear;
    private int year;
    private Map<Long, Integer> anyDays;
    
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
    public static final String  WEEK_DAY_OR_WEEK_END_OF_WEEK  = "weekDayOrWeekendOfWeek";
    public static final String  HOUR_RANGE_OF_WEEK_DAY  = "hourRangeOfWeekDay";
    public static final String  HOUR_RANGE_OF_WEEK_END_DAY  = "hourRangeOfWeekEndDay";
    public static final String  WEEK_OF_YEAR = "weekOfYear";
    public static final String  MONTH_OF_YEAR = "monthOfYear";
    public static final String  ANY_TIME_RANGE = "anyTimeRange";
    public static final String  ANY_DAY = "anyDay";
    
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
		
		//key:hour value:hour group
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
    	cycleIndex = -1;
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
    	} else if (seasonalCycleType.equals(WEEK_DAY_OR_WEEK_END_OF_WEEK)) {
        	parentCycleIndex = timeStamp / secInWeek;
    		cycleIndex = (int)((timeStamp % secInWeek) / secInDay);
    		cycleIndex = cycleIndex > 4 ?  1 : 0;
    	} else if (seasonalCycleType.equals(HOUR_OF_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
    		cycleIndex = (int)((timeStamp % secInDay) / secInHour);
    	} else if (seasonalCycleType.equals(HOUR_OF_WEEK_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
           	weekDayIndex = parentCycleIndex % 7;
           	cycleIndex = weekDayIndex < 5 ? (int)((timeStamp % secInDay) / secInHour) : -1;
    	}  else if (seasonalCycleType.equals(HOUR_OF_WEEK_END_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
        	cycleIndex = weekDayIndex > 4 ? (int)((timeStamp % secInDay) / secInHour) :  -1;
    	} else  if (seasonalCycleType.equals(HALF_HOUR_OF_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
    		cycleIndex = (int)((timeStamp % secInDay) / secInHalfHour);
    	} else  if (seasonalCycleType.equals(HALF_HOUR_OF_WEEK_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
        	weekDayIndex = parentCycleIndex % 7;
        	cycleIndex = weekDayIndex < 5 ? (int)((timeStamp % secInDay) / secInHalfHour) : -1;
    	} else  if (seasonalCycleType.equals(HALF_HOUR_OF_WEEK_END_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
        	weekDayIndex = parentCycleIndex % 7;
        	cycleIndex = weekDayIndex > 4 ? (int)((timeStamp % secInDay) / secInHalfHour) : -1;
    	} else  if (seasonalCycleType.equals(QUARTER_HOUR_OF_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
    		cycleIndex = (int)((timeStamp % secInDay) / secInQuarterHour);
    	} else  if (seasonalCycleType.equals(QUARTER_HOUR_OF_WEEK_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
        	weekDayIndex = parentCycleIndex % 7;
        	cycleIndex = weekDayIndex < 5 ? (int)((timeStamp % secInDay) / secInQuarterHour) : -1;
    	} else  if (seasonalCycleType.equals(QUARTER_HOUR_OF_WEEK_END_DAY)) {
        	parentCycleIndex = timeStamp / secInDay;
        	weekDayIndex = parentCycleIndex % 7;
        	cycleIndex = weekDayIndex > 4 ? (int)((timeStamp % secInDay) / secInQuarterHour) : -1;
    	} else  if (seasonalCycleType.equals(HOUR_RANGE_OF_WEEK_DAY)) {
    		parentCycleIndex = timeStamp / secInDay;
    		weekDayIndex = parentCycleIndex % 7;
    		if (weekDayIndex < 5) {
    			int hourCycleIndex = (int)((timeStamp % secInDay) / secInHour);
    			Integer hourGroup = hourRanges.get(hourCycleIndex);
    			cycleIndex = hourGroup != null ? hourGroup : -1;
    		} 
    	} else  if (seasonalCycleType.equals(HOUR_RANGE_OF_WEEK_END_DAY)) {
    		parentCycleIndex = timeStamp / secInDay;
    		weekDayIndex = parentCycleIndex % 7;
    		if (weekDayIndex >= 5) {
    			int hourCycleIndex = (int)((timeStamp % secInDay) / secInHour);
    			Integer hourGroup = hourRanges.get(hourCycleIndex);
    			cycleIndex = hourGroup != null ? hourGroup : -1;
    		} 
    	} else  if (seasonalCycleType.equals(MONTH_OF_YEAR)) {
    		monthOfYearCycleIndex(timeStamp);
    	} else  if (seasonalCycleType.equals(WEEK_OF_YEAR)) {
    		weekOfYearCycleIndex(timeStamp);
    	} else  if (seasonalCycleType.equals(ANY_TIME_RANGE)) {
    		int indx = 0;
    		for (Pair<Integer, Integer> timeRange :  timeRanges) {
    			if (timeStamp >= timeRange.getLeft() && timeStamp <= timeRange.getRight()) {
    				cycleIndex = indx;
    				break;
    			}
    			++indx;
    		}
    	} else  if (seasonalCycleType.equals(ANY_DAY)) {
    		parentCycleIndex = 0;
    		for (long  dayBegin :  anyDays.keySet()) {
    			long dayEnd = dayBegin + secInDay;
    			if (timeStamp >= dayBegin && timeStamp < dayEnd) {
    				cycleIndex = anyDays.get(dayBegin);
    				break;
    			}
    		}
    	} else {
    		throw new IllegalArgumentException("invalid cycle type");
    	}
    	
    	return cycleIndex;
    }

	/**
	 * @param hourRanges
	 */
	public void setHourRanges(Map<Integer, Integer> hourRanges) {
		this.hourRanges = hourRanges;
	}

	/**
	 * @param timeRanges
	 */
	public void setTimeRanges(List<Pair<Integer, Integer>> timeRanges) {
		this.timeRanges = timeRanges;
	}

	public void setAnyDays(Map<Long, Integer> anyDays) {
		this.anyDays = anyDays;
	}

	/**
	 * @param timeStampInMili
	 */
	public void setTimeStampInMili(boolean timeStampInMili) {
		this.timeStampInMili = timeStampInMili;
	}

	/**
	 * @param timeZoneShiftHours
	 */
	public void setTimeZoneShiftHours(long timeZoneShiftHours) {
		this.timeZoneShiftSec = timeZoneShiftHours * secInHour ;
	}

	/**
	 * @return
	 */
	public long getParentCycleIndex() {
		return parentCycleIndex;
	}
    
	/**
	 * @param timeStamp
	 */
	private  void  monthOfYearCycleIndex(long timeStamp) {
		//go up to year beginning
		secToYearBeginning(timeStamp);
		
		//month index
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
		parentCycleIndex = 0;
	}
	
	/**
	 * @param timeStamp
	 */
	private  void  weekOfYearCycleIndex(long timeStamp) {
		//go up to year beginning
		secToYearBeginning(timeStamp);
		
		//week into beginning of year
		long secToWeekYear = 0;
		long week = secToYear / secInWeek;
		long remainder = secToYear % secInWeek;
		week += (remainder > 0 ? 1 : 0);
		secToWeekYear = week * secInWeek;
		
		//week index
		cycleIndex = 0;
		for (; secToWeekYear < timeStamp; ++cycleIndex) {
			secToWeekYear += secInWeek;
		}
		parentCycleIndex = 0;
	}
	
	/**
	 * @param timeStamp
	 */
	private void  secToYearBeginning(long timeStamp) {
		//go upto year
		secToYear = 0;
		year = 1971;
		long secInCurYear = 0;
		for (; secToYear < timeStamp; ++year) {
			secToYear += secInYear;
			secInCurYear = secInYear;
			if (year % 4 == 0) {
				secToYear += secInDay;
				secInCurYear += secInDay;
			}
		}
		
		//back up to beginning of year
		secToYear -= secInCurYear;
	}	
	
	/**
	 * @return
	 */
	public  boolean isHourRange() {
		return seasonalCycleType.equals(HOUR_RANGE_OF_WEEK_DAY)  ||  
			seasonalCycleType.equals(HOUR_RANGE_OF_WEEK_END_DAY);
	}
	
	/**
	 * @return
	 */
	public boolean isAnyDay() {
		return seasonalCycleType.equals(ANY_DAY);
	}
	
	/**
	 * @param analyzers
	 * @param timeStamp
	 * @return
	 */
	public static Pair<String, Integer> getCycleIndex(SeasonalAnalyzer[] analyzers, long timeStamp) {
		Pair<String, Integer> seasonalCycle = new Pair<String, Integer>("normal", 0);
		for (SeasonalAnalyzer analyzer : analyzers) {
			int cycleIndex = analyzer.getCycleIndex(timeStamp);
			if (cycleIndex >= 0) {
				//first valid cycle only
				seasonalCycle.setLeft(analyzer.seasonalCycleType);
				seasonalCycle.setRight(cycleIndex);
				break;
			}
		}
		return seasonalCycle;
	}

	/**
	 * @param analyzer
	 * @param timeStamp
	 * @return
	 */
	public static Pair<String, Integer> getCycleIndex(SeasonalAnalyzer analyzer, long timeStamp) {
		Pair<String, Integer> seasonalCycle = new Pair<String, Integer>("normal", 0);
		int cycleIndex = analyzer.getCycleIndex(timeStamp);
		seasonalCycle.setLeft(analyzer.seasonalCycleType);
		seasonalCycle.setRight(cycleIndex);
		return seasonalCycle;
	}

	/**
	 * @param analyzers
	 * @param timeStamp
	 * @return
	 */
	public static List<Pair<String, Integer>> getCycleIndexes(SeasonalAnalyzer[] analyzers, long timeStamp) {
		List<Pair<String, Integer>> seasonalCycles = new ArrayList<Pair<String, Integer>>();
		for (SeasonalAnalyzer analyzer : analyzers) {
			int cycleIndex = analyzer.getCycleIndex(timeStamp);
			if (cycleIndex >= 0) {
				//all cycles
				Pair<String, Integer> seasonalCycle = new Pair<String, Integer>("normal", 0);
				seasonalCycle.setLeft(analyzer.seasonalCycleType);
				seasonalCycle.setRight(cycleIndex);
				seasonalCycles.add(seasonalCycle);
			}
		}
		return seasonalCycles;
	}
	
}
