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
			dateFormat =   new SimpleDateFormat(config.getString("dateFormat"));
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
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public ElapsedTimeTransformer(ProcessorAttribute prAttr, Config config) {
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
		public ElapsedTimeTransformer(String dateFormat, String timeZone, String timeUnit, boolean failOnInvalid, 
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
				if (time > refTime) {
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
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public ContextualElapsedTimeTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			refDateFieldOrdinal = config.getInt("refDateFieldOrdinal");
			intialize(config.getString("dateFormat"), config.getString("timeZone"),
					config.getString("timeUnit"), config.getBoolean("failOnInvalid"));

		}
		
		/**
		 * @param dateFormat
		 * @param timeZone
		 * @param timeUnit
		 * @param failOnInvalid
		 */
		public ContextualElapsedTimeTransformer(String dateFormat, String timeZone, String timeUnit, boolean failOnInvalid) {
			super(1);
			intialize(dateFormat,  timeZone, timeUnit, failOnInvalid);
		}

		/**
		 * @param dateFormatStr
		 * @param timeZone
		 * @param timeUnit
		 * @param failOnInvalid
		 */
		private void intialize(String dateFormatStr, String timeZone, String timeUnit, boolean failOnInvalid) {
			if (dateFormatStr.equals("epochTime")) {
				epochTime = true;
			} else  {
				dateFormat = new SimpleDateFormat(dateFormatStr);
				if (!Utility.isBlank(timeZone)) {
					dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
				}
			}
			this.timeUnit = timeUnit;
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
				if (time > refTime) {
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
					} else if (timeUnit.equals(BasicUtils.TIME_UNIT_YEAR)) {
						date.add(Calendar.YEAR, 1);
					} else {
						throw new IllegalStateException("invalid time cycle unit for time shift");
					}
				}
				transformed[0] = dateFormat.format(date);
			} catch (ParseException ex) {
				throw new IllegalArgumentException("failed to parse date " + ex.getMessage());
			}
			return transformed;
		}		
	}	
	
}
