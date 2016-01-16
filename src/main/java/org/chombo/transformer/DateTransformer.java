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
import java.util.TimeZone;

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
	
}
