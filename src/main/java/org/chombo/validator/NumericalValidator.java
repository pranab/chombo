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

import java.util.Map;

import org.chombo.util.AttributeSchema;

/**
 * @author pranab
 *
 */
public class NumericalValidator {
	
	/**
	 * @author pranab
	 *
	 */
	public static class IntMinValidator extends Validator {

		public IntMinValidator(String tag, int ordinal, AttributeSchema schema) {
			super(tag, ordinal, schema);
		}

		@Override
		public boolean isValid(String value) {
			int intValue = Integer.parseInt(value);
			return intValue >= attribute.getMin();
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class IntMaxValidator extends Validator {

		public IntMaxValidator(String tag, int ordinal, AttributeSchema schema) {
			super(tag, ordinal, schema);
		}

		@Override
		public boolean isValid(String value) {
			int intValue = Integer.parseInt(value);
			return intValue <= attribute.getMax();
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class StatsBasedIntRangeValidator extends Validator {
		private int min;
		private int max;
		
		public StatsBasedIntRangeValidator(String tag, int ordinal, AttributeSchema schema, Map<String, Object> validatorContext) {
			super(tag, ordinal, schema);
			double mean = (Double)validatorContext.get("mean:" + ordinal);
			double stdDev = (Double)validatorContext.get("stdDev:" + ordinal);
			min = (int)(mean - attribute.getMaxZscore() * stdDev);
			max = (int)(mean + attribute.getMaxZscore() * stdDev);
		}

		@Override
		public boolean isValid(String value) {
			int intValue = Integer.parseInt(value);
			return intValue >= min && intValue <= max;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class DoubleMinValidator extends Validator {

		public DoubleMinValidator(String tag, int ordinal, AttributeSchema schema) {
			super(tag, ordinal, schema);
		}

		@Override
		public boolean isValid(String value) {
			double dblValue = Double.parseDouble(value);
			return dblValue >= attribute.getMin();
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class DoubleMaxValidator extends Validator {

		public DoubleMaxValidator(String tag, int ordinal, AttributeSchema schema) {
			super(tag, ordinal, schema);
		}

		@Override
		public boolean isValid(String value) {
			double dblValue = Double.parseDouble(value);
			return dblValue <= attribute.getMax();
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class StatsBasedDoubleRangeValidator extends Validator {
		private double min;
		private double max;
		
		public StatsBasedDoubleRangeValidator(String tag, int ordinal, AttributeSchema schema, Map<String, Object> validatorContext) {
			super(tag, ordinal, schema);
			double mean = (Double)validatorContext.get("mean:" + ordinal);
			double stdDev = (Double)validatorContext.get("stdDev:" + ordinal);
			min = mean - attribute.getMaxZscore() * stdDev;
			max = mean + attribute.getMaxZscore() * stdDev;
		}

		@Override
		public boolean isValid(String value) {
			double dblValue = Double.parseDouble(value);
			return dblValue >= min && dblValue <= max;
		}
	}
	
}
