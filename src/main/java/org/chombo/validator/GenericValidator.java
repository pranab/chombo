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

import org.chombo.util.ProcessorAttribute;

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
	public static class EnsureIntValidator extends Validator {
		
		public EnsureIntValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			boolean valid = true;
			try {
				int intVal = Integer.parseInt(value);
			} catch (Exception ex) {
				valid = false;
			}
			return valid;
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
			boolean valid = true;
			try {
				long longVal = Long.parseLong(value);
			} catch (Exception ex) {
				valid = false;
			}
			return valid;
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
			boolean valid = true;
			try {
				double dblVal = Double.parseDouble(value);
			} catch (Exception ex) {
				valid = false;
			}
			return valid;
		}
	}
}
