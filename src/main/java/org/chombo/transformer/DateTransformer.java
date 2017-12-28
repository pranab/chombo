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

package org.chombo.transformer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.chombo.util.BasicUtils;
import org.chombo.util.ProcessorAttribute;
import org.chombo.util.Utility;

import com.typesafe.config.Config;

/**
 * @author pranab
 *
 */
public class DateTransformer  {
	
	/**
	 * @author pranab
	 *
	 */
	public static class EpochTimeGenerator extends AttributeTransformer {

		/**
		 * @param prAttr
		 */
		public EpochTimeGenerator(ProcessorAttribute prAttr) {
			super(prAttr.getTargetFieldOrdinals().length);
		}
		
		public EpochTimeGenerator() {
			super(1);
		}

		@Override
		public String[] tranform(String value) {
			transformed[0] = "" + System.currentTimeMillis();
			return transformed;
		}
		
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class DateGenerator extends AttributeTransformer {
		private SimpleDateFormat dateFormat;
		
		/**
		 * @param prAttr
		 */
		public DateGenerator(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			dateFormat = new SimpleDateFormat(config.getString("dateFormat"));
		}
		
		public DateGenerator(String dateFormat) {
			super(1);
			this.dateFormat = new SimpleDateFormat(dateFormat);
		}

		@Override
		public String[] tranform(String value) {
			transformed[0] = dateFormat.format(Calendar.getInstance().getTime());
			return transformed;
		}
	}	

	/**
	 * @author pranab
	 *
	 */
	public static class DateFormatTransformer extends AttributeTransformer {
		private SimpleDateFormat sourceDateFormat;
		private SimpleDateFormat targetDateFormat;
		private boolean sourceEpochTime;
		private boolean targetEpochTime;
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public DateFormatTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			intialize(config.getString("sourceDateFormat"), config.getString("sourceTimeZone"), config.getString("targetDateFormat"), 
					config.getString("targetTimeZone"));
		}
		
		/**
		 * @param sourceDateFormat
		 * @param sourceTimeZone
		 * @param targetDateFormat
		 * @param targetTimeZone
		 */
		public DateFormatTransformer(String sourceDateFormat, String sourceTimeZone, String targetDateFormat, String targetTimeZone) {
			super(1);
			intialize(sourceDateFormat,  sourceTimeZone, targetDateFormat,  targetTimeZone);
		}

		/**
		 * @param sourceDateFormatStr
		 * @param sourceTimeZone
		 * @param targetDateFormatStr
		 * @param targetTimeZone
		 */
		private void intialize(String sourceDateFormatStr, String sourceTimeZone, String targetDateFormatStr, String targetTimeZone) {
			if (sourceDateFormatStr.equals("epochTime")) {
				sourceEpochTime = true;
			} else  {
				sourceDateFormat = new SimpleDateFormat(sourceDateFormatStr);
				if (!Utility.isBlank(sourceTimeZone)) {
					sourceDateFormat.setTimeZone(TimeZone.getTimeZone(sourceTimeZone));
				}
			}

			if (targetDateFormatStr.equals("epochTime")) {
				targetEpochTime = true;
			} else  {
				targetDateFormat = new SimpleDateFormat(targetDateFormatStr);
				if (!Utility.isBlank(targetTimeZone)) {
					targetDateFormat.setTimeZone(TimeZone.getTimeZone(targetTimeZone));
				}
			}
		}

		@Override
		public String[] tranform(String value) {
			try {
				Date date = null;
				if (null != sourceDateFormat) {
					//date format
					date = sourceDateFormat.parse(value);
				} else {
					//epoch time
					date = new Date(Long.parseLong(value));
				}
				
				if (null != targetDateFormat) {
					transformed[0] = targetDateFormat.format(date);
				} else {
					transformed[0] =  "" + date.getTime();
				}
			} catch (ParseException ex) {
				throw new IllegalArgumentException("failed to parse date " + ex.getMessage());
			}
			return transformed;
		}
	}	
	
	
	/**
	 * @author pranab
	 *
	 */
	public static class ElapsedTimeTransformer extends AttributeTransformer  {
		private SimpleDateFormat dateFormat;
		private boolean epochTime;
		private long refTime;
		private String timeUnit;
		private boolean failOnInvalid;
		private boolean refTimeAlwaysBehind;
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public ElapsedTimeTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			String refDateStr = config.hasPath("refDateStr")? config.getString("refDateStr") :  null;
			intialize(config.getString("dateFormat"), config.getString("timeZone"),
					config.getString("timeUnit"), config.getBoolean("failOnInvalid"), 
					config.getBoolean("refTimeAlwaysBehind"), refDateStr);
		}
		
		/**
		 * @param dateFormat
		 * @param timeZone
		 * @param failOnInvalid
		 */
		public ElapsedTimeTransformer(String dateFormat, String timeZone, String timeUnit, boolean failOnInvalid, 
			boolean refTimeAlwaysBehind, String refDateStr) {
			super(1);
			intialize(dateFormat,  timeZone, timeUnit, failOnInvalid, refTimeAlwaysBehind, refDateStr);
		}

		/**
		 * @param dateFormatStr
		 * @param timeZone
		 * @param failOnInvalid
		 */
		private void intialize(String dateFormatStr, String timeZone, String timeUnit, boolean failOnInvalid, 
				boolean refTimeAlwaysBehind, String refDateStr) {
			try {
				if (dateFormatStr.equals("epochTime")) {
					epochTime = true;
				} else  {
					dateFormat = new SimpleDateFormat(dateFormatStr);
					if (!Utility.isBlank(timeZone)) {
						dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
					}
				}
				
				//set reference time
				if (null != refDateStr) {
					if (epochTime) {
						refTime = Long.parseLong(refDateStr);
					} else {
						Date refDate = dateFormat.parse(refDateStr);
						refTime = refDate.getTime();
					}
				} else {
					refTime = System.currentTimeMillis();
				}
				this.timeUnit = timeUnit;
				this.refTimeAlwaysBehind = refTimeAlwaysBehind;
			} catch (ParseException ex) {
				throw new IllegalArgumentException("failed to parse date " + ex.getMessage());
			}
		}

		@Override
		public String[] tranform(String value) {
			long elapsed  = 0;
			long time = 0;
			try {
				Date date = null;
				if (null != dateFormat) {
					//date format
					date = dateFormat.parse(value);
					time = date.getTime();
				} else {
					//epoch time
					time = Long.parseLong(value);
				}
				if (!refTimeAlwaysBehind || time > refTime) {
					elapsed = time - refTime;
					elapsed = BasicUtils.convertTimeUnit(elapsed, timeUnit);
					transformed[0] =  "" + elapsed;
				} else {
					if (failOnInvalid) {
						throw new IllegalArgumentException("date in future");
					} else {
						transformed[0] =  "0";
					}
				}
			} catch (ParseException ex) {
				throw new IllegalArgumentException("failed to parse date " + ex.getMessage());
			}
			return transformed;
		}
	}	
	
	/**
	 * @author pranab
	 *
	 */
	public static class ContextualElapsedTimeTransformer extends AttributeTransformer implements ContextAwareTransformer {
		private String[] fields;
		private boolean epochTime;
		private long refTime;
		private String timeUnit;
		private boolean failOnInvalid;
		private SimpleDateFormat dateFormat;
		private int refDateFieldOrdinal;
		private boolean refTimeAlwaysBehind;
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public ContextualElapsedTimeTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			refDateFieldOrdinal = config.getInt("refDateFieldOrdinal");
			intialize(config.getString("dateFormat"), config.getString("timeZone"),
					config.getString("timeUnit"), config.getBoolean("failOnInvalid"), config.getBoolean("refTimeAlwaysBehind"));

		}
		
		/**
		 * @param dateFormat
		 * @param timeZone
		 * @param timeUnit
		 * @param failOnInvalid
		 */
		public ContextualElapsedTimeTransformer(String dateFormat, String timeZone, String timeUnit, boolean failOnInvalid,
				boolean refTimeAlwaysBehind) {
			super(1);
			intialize(dateFormat,  timeZone, timeUnit, failOnInvalid, refTimeAlwaysBehind);
		}

		/**
		 * @param dateFormatStr
		 * @param timeZone
		 * @param timeUnit
		 * @param failOnInvalid
		 */
		private void intialize(String dateFormatStr, String timeZone, String timeUnit, boolean failOnInvalid, 
				boolean refTimeAlwaysBehind) {
			if (dateFormatStr.equals("epochTime")) {
				epochTime = true;
			} else  {
				dateFormat = new SimpleDateFormat(dateFormatStr);
				if (!Utility.isBlank(timeZone)) {
					dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
				}
			}
			this.timeUnit = timeUnit;
			this.refTimeAlwaysBehind = refTimeAlwaysBehind;
		}
		
		@Override
		public void setContext(Map<String, Object> context) {
			fields = (String[])context.get("record");
		}

		@Override
		public String[] tranform(String value) {
			long elapsed  = 0;
			long time = 0;
			try {
				//reference date time
				String refDateStr = fields[refDateFieldOrdinal];
				if (epochTime) {
					refTime = Long.parseLong(refDateStr);
				} else {
					Date refDate = dateFormat.parse(refDateStr);
					refTime = refDate.getTime();
				}

				Date date = null;
				if (null != dateFormat) {
					//date format
					date = dateFormat.parse(value);
					time = date.getTime();
				} else {
					//epoch time
					time = Long.parseLong(value);
				}
				if (!refTimeAlwaysBehind || time > refTime) {
					elapsed = time - refTime;
					elapsed = BasicUtils.convertTimeUnit(elapsed, timeUnit);
					transformed[0] =  "" + elapsed;
				} else {
					if (failOnInvalid) {
						throw new IllegalArgumentException("date in future");
					} else {
						transformed[0] =  "0";
					}
				}
			} catch (ParseException ex) {
				throw new IllegalArgumentException("failed to parse date " + ex.getMessage());
			}
			return transformed;
		}
		
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class TimeCyclicShiftTransformer extends AttributeTransformer  {
		private SimpleDateFormat dateFormat;
		private boolean epochTime;
		private long refTime;
		private String timeUnit;
		private boolean failOnInvalid;
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public TimeCyclicShiftTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			String refDateStr = config.hasPath("refDateStr")? config.getString("refDateStr") :  null;
			intialize(config.getString("dateFormat"), config.getString("timeZone"),
					config.getString("timeUnit"), config.getBoolean("failOnInvalid"), refDateStr);
		}
		
		/**
		 * @param dateFormat
		 * @param timeZone
		 * @param failOnInvalid
		 */
		public TimeCyclicShiftTransformer(String dateFormat, String timeZone, String timeUnit, boolean failOnInvalid, 
			String refDateStr) {
			super(1);
			intialize(dateFormat,  timeZone, timeUnit, failOnInvalid, refDateStr);
		}

		/**
		 * @param dateFormatStr
		 * @param timeZone
		 * @param failOnInvalid
		 */
		private void intialize(String dateFormatStr, String timeZone, String timeUnit, boolean failOnInvalid, 
			String refDateStr) {
			try {
				dateFormat = new SimpleDateFormat(dateFormatStr);
				if (!Utility.isBlank(timeZone)) {
					dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
				}
				
				//set reference time
				if (null != refDateStr) {
					Date refDate = dateFormat.parse(refDateStr);
					refTime = refDate.getTime();
				} else {
					refTime = System.currentTimeMillis();
				}
				this.timeUnit = timeUnit;
			} catch (ParseException ex) {
				throw new IllegalArgumentException("failed to parse date " + ex.getMessage());
			}
		}
		
		@Override
		public String[] tranform(String value) {
			try {
				Calendar date = Calendar.getInstance();
				date.setTime(dateFormat.parse(value));
				for (long time = date.getTimeInMillis(); time < refTime; time = date.getTimeInMillis()) {
					if (timeUnit.equals(BasicUtils.TIME_UNIT_MONTH)) {
						date.add(Calendar.MONTH, 1);
					} else if (timeUnit.equals(BasicUtils.TIME_UNIT_QUARTER)) {
						date.add(Calendar.MONTH, 3);
					} else if (timeUnit.equals(BasicUtils.TIME_UNIT_SEMI_ANNUAL)) {
						date.add(Calendar.MONTH, 6);
					} else if (timeUnit.equals(BasicUtils.TIME_UNIT_YEAR)) {
						date.add(Calendar.YEAR, 1);
					} else {
						throw new IllegalStateException("invalid time cycle unit for time shift");
					}
				}
				transformed[0] = dateFormat.format(date.getTime());
			} catch (ParseException ex) {
				throw new IllegalArgumentException("failed to parse date " + ex.getMessage());
			}
			return transformed;
		}		
	}	
	
	
	/**
	 * @author pranab
	 *
	 */
	public static class ContextualTimeCyclicShiftTransformer extends AttributeTransformer implements ContextAwareTransformer {
		private SimpleDateFormat dateFormat;
		private long refTime;
		private String timeUnit;
		private String[] fields;
		private int timeUnitColOrd;
		private int refDateColOrd;
		private boolean epochTime;
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public ContextualTimeCyclicShiftTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			int refDateColOrd = config.hasPath("refDateColOrd")? config.getInt("refDateColOrd") :  -1;
			String refDateStr = config.hasPath("refDateStr")? config.getString("refDateStr") :  null;
			int timeUnitColOrd = config.hasPath("timeUnitColOrd")? config.getInt("timeUnitColOrd") :  -1;
			String timeUnit = config.hasPath("timeUnit")? config.getString("timeUnit") :  null;
			intialize(config.getString("dateFormat"), config.getString("timeZone"),
					timeUnitColOrd, timeUnit, config.getBoolean("failOnInvalid"), refDateColOrd, refDateStr);
		}
		
		/**
		 * @param dateFormat
		 * @param timeZone
		 * @param failOnInvalid
		 */
		public ContextualTimeCyclicShiftTransformer(String dateFormat, String timeZone, int timeUnitColOrd, 
			String timeUnit, boolean failOnInvalid, int refDateColOrd, String refDateStr) {
			super(1);
			intialize(dateFormat,  timeZone, timeUnitColOrd, timeUnit, failOnInvalid, refDateColOrd, refDateStr);
		}

		/**
		 * @param dateFormatStr
		 * @param timeZone
		 * @param failOnInvalid
		 */
		private void intialize(String dateFormatStr, String timeZone, int timeUnitColOrd, String timeUnit, 
			boolean failOnInvalid, int refDateColOrd, String refDateStr) {
			try {
				//date format
				if (dateFormatStr.equals("epochTime")) {
					epochTime = true;
				} else {
					dateFormat = new SimpleDateFormat(dateFormatStr);
					if (!Utility.isBlank(timeZone)) {
						dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
					}
				}
				
				//set configured reference time
				if (null != refDateStr) {
					refTime = BasicUtils.getEpochTime(refDateStr,  dateFormat);
				} 
				this.timeUnit = timeUnit;
				this.timeUnitColOrd = timeUnitColOrd;
				this.refDateColOrd = refDateColOrd;
			} catch (ParseException ex) {
				throw new IllegalArgumentException("failed to parse date " + ex.getMessage());
			}
		}
		
		@Override
		public String[] tranform(String value) {
			try {
				//reference date
				long thisRefTime = 0;
				if (refDateColOrd >= 0) {
					//from a field in data
					thisRefTime = BasicUtils.getEpochTime(fields[refDateColOrd],  dateFormat);
				} else if (refTime > 0) {
					//from configuration
					thisRefTime = refTime;
				} else {
					throw new IllegalStateException("either reference date column index or global ref date must be provided");
				}
				
				//time unit
				String thisTimeUnit = null;
				if (timeUnitColOrd >= 0) {
					thisTimeUnit = fields[timeUnitColOrd];
				} else if (null != timeUnit) {
					thisTimeUnit = timeUnit;
				} else {
					throw new IllegalStateException("either cycle time unit column index or global cycle time unit must be provided");
				}
				
				//roll forward
				Calendar date = Calendar.getInstance();
				date.setTimeInMillis(BasicUtils.getEpochTime(value, dateFormat));
				for (long time = date.getTimeInMillis(); time < thisRefTime; time = date.getTimeInMillis()) {
					if (thisTimeUnit.equals(BasicUtils.TIME_UNIT_MONTH)) {
						date.add(Calendar.MONTH, 1);
					} else if (thisTimeUnit.equals(BasicUtils.TIME_UNIT_QUARTER)) {
						date.add(Calendar.MONTH, 3);
					} else if (thisTimeUnit.equals(BasicUtils.TIME_UNIT_YEAR)) {
						date.add(Calendar.YEAR, 1);
					} else {
						throw new IllegalStateException("invalid time cycle unit for time shift");
					}
				}
				transformed[0] = null != dateFormat ? dateFormat.format(date.getTime()) : 
					"" + date.getTimeInMillis();
			} catch (ParseException ex) {
				throw new IllegalArgumentException("failed to parse date " + ex.getMessage());
			}
			return transformed;
		}		
		
		@Override
		public void setContext(Map<String, Object> context) {
			fields = (String[])context.get("record");
		}
		
	}	
	
	
	/**
	 * @author pranab
	 *
	 */
	public static class DateComponentTransformer extends AttributeTransformer {
		private SimpleDateFormat sourceDateFormat;
		private Map<String, SimpleDateFormat> componentDateFormats = new HashMap<String, SimpleDateFormat>();
		private boolean sourceEpochTime;
		private List<String> dateComponents;
		private Calendar cal = Calendar.getInstance();

		/**
		 * @param prAttr
		 * @param config
		 */
		public DateComponentTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			List<String> dateComponents = config.getStringList("dateComponenets");
			
			//component format string
			Map<String, String> componentFormats = new HashMap<String, String>();
			for (String dateComponent : dateComponents) {
				if (!dateComponent.equals(BasicUtils.TIME_UNIT_QUARTER)) {
					String compFormat = config.getString("format." + dateComponent);
					componentFormats.put(dateComponent, compFormat);
				}
			}
			
			intialize(config.getString("sourceDateFormat"), config.getString("sourceTimeZone"),  
					config.getStringList("dateComponenets"), componentFormats, config.getString("targetTimeZone"));
		}
		
		/**
		 * @param sourceDateFormat
		 * @param sourceTimeZone
		 * @param dateComponents
		 * @param targetComponentFormats
		 * @param targetTimeZone
		 */
		public DateComponentTransformer(String sourceDateFormat, String sourceTimeZone, List<String> dateComponents, 
				Map<String, String> targetComponentFormats, 
				String targetTimeZone) {
			super(dateComponents.size());
			intialize(sourceDateFormat,  sourceTimeZone, dateComponents, targetComponentFormats,  targetTimeZone);
		}

		/**
		 * @param sourceDateFormatStr
		 * @param sourceTimeZone
		 * @param dateComponents
		 * @param componentFormats
		 * @param targetTimeZone
		 */
		private void intialize(String sourceDateFormatStr, String sourceTimeZone, List<String> dateComponents,  
				Map<String, String> componentFormats, String targetTimeZone) {
			this.dateComponents = dateComponents;
			if (sourceDateFormatStr.equals("epochTime")) {
				sourceEpochTime = true;
			} else  {
				sourceDateFormat = new SimpleDateFormat(sourceDateFormatStr);
				if (!Utility.isBlank(sourceTimeZone)) {
					sourceDateFormat.setTimeZone(TimeZone.getTimeZone(sourceTimeZone));
				}
			}

			//format object for all components
			for (Map.Entry<String,String> entry :componentFormats.entrySet()) {
				SimpleDateFormat targetDateFormat = new SimpleDateFormat(entry.getValue());
				if (!Utility.isBlank(targetTimeZone)) {
					targetDateFormat.setTimeZone(TimeZone.getTimeZone(targetTimeZone));
				}
				componentDateFormats.put(entry.getKey(), targetDateFormat);
			}
		}

		@Override
		public String[] tranform(String value) {
			try {
				Date date = null;
				if (null != sourceDateFormat) {
					//date format
					date = sourceDateFormat.parse(value);
				} else {
					//epoch time
					date = new Date(Long.parseLong(value));
				}
				
				int i = 0;
				for (String dateComponent : dateComponents) {
					if (dateComponent.equals(BasicUtils.TIME_UNIT_QUARTER)) {
						cal.setTime(date);
						int month = cal.get(Calendar.MONTH);
						int quarter = month / 3 + 1;
						transformed[i++] = "" + quarter;
					} else {
						SimpleDateFormat targetDateFormat = componentDateFormats.get(dateComponent);
						transformed[i++] = targetDateFormat.format(date);
					}
				}
			} catch (ParseException ex) {
				throw new IllegalArgumentException("failed to parse date " + ex.getMessage());
			}
			return transformed;
		}
	}	
	
	/**
	 * @author pranab
	 *
	 */
	public static class TimeCycleTransformer extends AttributeTransformer  {
		private SimpleDateFormat sourceDateFormat;
		private boolean sourceEpochTime;
		private String timeCycle;
		private int hourGranularity;
		private Calendar cal = Calendar.getInstance();
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public TimeCycleTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			intialize(config.getString("sourceDateFormat"), config.getString("sourceTimeZone"), config.getString("timeCycle"), 
					config.getInt("hourGranularity"));

		}
		
		/**
		 * @param sourceDateFormatStr
		 * @param sourceTimeZone
		 * @param timeCycle
		 * @param hourGranularity
		 */
		public TimeCycleTransformer(String sourceDateFormatStr, String sourceTimeZone, String timeCycle, int hourGranularity) {
			super(1);
			intialize(sourceDateFormatStr, sourceTimeZone, timeCycle, hourGranularity);
		}
		
		/**
		 * @param sourceDateFormatStr
		 * @param sourceTimeZone
		 * @param timeCycle
		 * @param hourGranularity
		 */
		private void intialize(String sourceDateFormatStr, String sourceTimeZone, String timeCycle, int hourGranularity) {
			if (sourceDateFormatStr.equals("epochTime")) {
				sourceEpochTime = true;
			} else  {
				sourceDateFormat = new SimpleDateFormat(sourceDateFormatStr);
				if (!Utility.isBlank(sourceTimeZone)) {
					sourceDateFormat.setTimeZone(TimeZone.getTimeZone(sourceTimeZone));
				}
			}
			this.timeCycle = timeCycle;
			if (hourGranularity % 2 == 1) {
				throw new IllegalStateException("hour granularity should be even");
			}
			this.hourGranularity = hourGranularity;
		}
		
		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
		@Override
		public String[] tranform(String value) {
			try {
				Date date = null;
				if (null != sourceDateFormat) {
					//date format
					date = sourceDateFormat.parse(value);
				} else {
					//epoch time
					date = new Date(Long.parseLong(value));
				}
				cal.setTime(date);
				
				if (timeCycle.equals("hourOfDay")) {
					int hour = cal.get(Calendar.HOUR_OF_DAY);
					hour /= hourGranularity;
					transformed[0] = "" + hour;
				} else if (timeCycle.equals("dayOfWeek")) {
					int day = cal.get(Calendar.DAY_OF_WEEK);
					transformed[0] = "" + day;
				} else if (timeCycle.equals("dayOfMonth")) {
					int day = cal.get(Calendar.DAY_OF_MONTH);
					transformed[0] = "" + day;
				} else if (timeCycle.equals("monthOfYear")) {
					int month = cal.get(Calendar.MONTH) + 1;
					transformed[0] = "" + month;
				} else if (timeCycle.equals("quarterOfYear")) {
					int month = cal.get(Calendar.MONTH) + 1;
					int quarter = (month + 2) / 3;
					transformed[0] = "" + quarter;
				} else {
					throw new IllegalStateException("invalid time cycle");
				}
				
			} catch (ParseException ex) {
				throw new IllegalArgumentException("failed to parse date " + ex.getMessage());
			}
			return transformed;
		}
	
	}

	
}
