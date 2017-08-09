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

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.chombo.util.BasicUtils;
import org.chombo.util.ProcessorAttribute;

import com.typesafe.config.Config;

/**
 * @author pranab
 *
 */
public class GenericValidator {

	/**
	 * @author pranab
	 *
	 */
	public static class NotMissingValidator extends Validator {
		
		public NotMissingValidator(String tag,ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return !value.isEmpty();
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class NotMissingGroupValidator extends Validator {
		private List<int[]> fieldGroups;
		
		public NotMissingGroupValidator(String tag,Map<String, Object> validatorContext) {
			super(tag, null);
			fieldGroups = (List<int[]>)validatorContext.get("fieldGroups");
		}

		public NotMissingGroupValidator(String tag,Config validatorConfig) {
			//TODO
			super(tag, null);
		}
		
		@Override
		public boolean isValid(String value) {
			boolean isValid = true;
			String[] items = value.split(fieldDelim);
			
			//all groups
			for (int[] group : fieldGroups) {
				boolean grIsValid = false;
				for (int ord : group) {
					//at least one non empty in a group
					if (!items[ord].isEmpty()) {
						grIsValid = true;
						break;
					}
				}
				
				//all group validity
				isValid = isValid && grIsValid;
				if (!isValid) {
					break;
				}
			}
			return isValid;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class EnsureIntValidator extends Validator {
		
		public EnsureIntValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return BasicUtils.isInt(value);
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class EnsureLongValidator extends Validator {
		
		public EnsureLongValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return BasicUtils.isLong(value);
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class EnsureDoubleValidator extends Validator {
		
		public EnsureDoubleValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return BasicUtils.isDouble(value);
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class EnsureDateValidator extends Validator {
		private SimpleDateFormat dateFormatter;
		boolean epochTimeMs;
		
		public EnsureDateValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
			String datePattern = prAttr.getDatePattern();
			if (datePattern.equals(BasicUtils.EPOCH_TIME)) {
				epochTimeMs = true;
			} else if (datePattern.equals(BasicUtils.EPOCH_TIME_SEC)) {
				epochTimeMs = false;
			} else {
				dateFormatter = new SimpleDateFormat(prAttr.getDatePattern());
			}
		}

		@Override
		public boolean isValid(String value) {
			boolean valid = false;
			if (null != dateFormatter) {
				valid =  BasicUtils.isDate(value, dateFormatter);
			} else if (epochTimeMs) {
				valid = BasicUtils.isLong(value) && value.length() >= 13;
			} else {
				valid = BasicUtils.isLong(value) && value.length() >= 10;
			}
			
			return valid;
		}
	}
	
}
