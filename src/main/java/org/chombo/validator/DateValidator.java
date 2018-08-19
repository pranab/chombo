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

package org.chombo.validator;

import java.util.Date;

import org.chombo.util.ProcessorAttribute;

/**
 * @author pranab
 *
 */
public class DateValidator {
	
	/**
	 * @author pranab
	 *
	 */
	public static class DateMinValidator extends Validator {
		private DateValidatorHelper helper;
		
		public DateMinValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
			helper = new DateValidatorHelper(prAttr);
		}

		@Override
		public boolean isValid(String value) {
			boolean valid = false;
			try {
				if (!value.isEmpty()) {
					if (null != helper.getDateFormatter()) {
						Date date = helper.getDateFormatter().parse(value);
						Date earliestDate = helper.getDateFormatter().parse(prAttr.getEarliestDate());
						valid = date.after(earliestDate);
					} else  {
						long epoch = Long.parseLong(value);
						long earliestEpoch = Long.parseLong(prAttr.getEarliestDate());
						valid = epoch > earliestEpoch;
					} 
				} else {
					System.out.println("empty field in DateMinValidator");
				}
			} catch (Exception ex) {
				System.out.println("date formatting error in DateMinValidator for " + value + " " + ex.getMessage());
			}
			return valid;
		}
		
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class DateMaxValidator extends Validator {
		private DateValidatorHelper helper;
		
		public DateMaxValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
			helper = new DateValidatorHelper(prAttr);
		}

		@Override
		public boolean isValid(String value) {
			boolean valid = false;
			try {
				if (!value.isEmpty()) {
					if (null != helper.getDateFormatter()) {
						Date date = helper.getDateFormatter().parse(value);
						Date latestDate = helper.getDateFormatter().parse(prAttr.getLatestDate());
						valid = date.before(latestDate);
					} else  {
						long epoch = Long.parseLong(value);
						long latestEpoch = Long.parseLong(prAttr.getLatestDate());
						valid = epoch < latestEpoch;
					} 
				} else {
					System.out.println("empty field in DateMaxValidator");
				}
			} catch (Exception ex) {
				System.out.println("date formatting error in DateMaxValidator for " + value + " " + ex.getMessage());
			}
			return valid;
		}
		
	}
	
}
