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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.chombo.util.AttributeSchema;

/**
 * @author pranab
 *
 */
public class StringValidator {

	/**
	 * @author pranab
	 *
	 */
	public static class MinLengthValidator extends Validator {

		public MinLengthValidator(String tag, int ordinal, AttributeSchema schema) {
			super(tag, ordinal, schema);
		}

		@Override
		public boolean isValid(String value) {
			return value.length() >= attribute.getMin();
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class MaxLengthValidator extends Validator {

		public MaxLengthValidator(String tag, int ordinal, AttributeSchema schema) {
			super(tag, ordinal, schema);
		}

		@Override
		public boolean isValid(String value) {
			return value.length() <= attribute.getMax();
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class MinValidator extends Validator {

		public MinValidator(String tag, int ordinal, AttributeSchema schema) {
			super(tag, ordinal, schema);
		}

		@Override
		public boolean isValid(String value) {
			return value.compareTo(attribute.getMinString()) >= 0;
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class MaxValidator extends Validator {

		public MaxValidator(String tag, int ordinal, AttributeSchema schema) {
			super(tag, ordinal, schema);
		}

		@Override
		public boolean isValid(String value) {
			return value.compareTo(attribute.getMaxString()) <= 0;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class PatternValidator extends Validator {
		private Pattern pattern;
		
		public PatternValidator(String tag, int ordinal, AttributeSchema schema) {
			super(tag, ordinal, schema);
		}

		@Override
		public boolean isValid(String value) {
			if (null == pattern) {
				pattern = Pattern.compile(attribute.getStringPattern());
			}
			Matcher matcher = pattern.matcher(value);
			return matcher.matches();
		}
	}
}
