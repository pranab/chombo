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
import org.chombo.util.BaseAttribute;
import org.chombo.util.ProcessorAttribute;

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

		public MinLengthValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return value.length() >= prAttr.getMinLength();
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class MaxLengthValidator extends Validator {

		public MaxLengthValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return value.length() <= prAttr.getMaxLength();
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class LengthValidator extends Validator {

		public LengthValidator(String tag,ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return value.length() == prAttr.getLength();
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class MinValidator extends Validator {

		public MinValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return value.compareTo(prAttr.getMinString()) >= 0;
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class MaxValidator extends Validator {

		public MaxValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return value.compareTo(prAttr.getMaxString()) <= 0;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class PatternValidator extends Validator {
		protected Pattern pattern;
		
		public PatternValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			if (null == pattern) {
				pattern = Pattern.compile(prAttr.getStringPattern());
			}
			Matcher matcher = pattern.matcher(value);
			return matcher.matches();
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class PreDefinedPatternValidator extends PatternValidator {
		
		public PreDefinedPatternValidator(String tag, ProcessorAttribute prAttr) {
			super(tag, prAttr);
		}

		@Override
		public boolean isValid(String value) {
			if (null == pattern) {
				String patternName = prAttr.getStringPatternName();
				String patternString = BaseAttribute.getPatternString(patternName);
				if (null == patternString) {
					throw new IllegalStateException("invalid pattern name " + patternName);
				}
				pattern = Pattern.compile(patternString);
			}
			Matcher matcher = pattern.matcher(value);
			return matcher.matches();
		}
	}
}
