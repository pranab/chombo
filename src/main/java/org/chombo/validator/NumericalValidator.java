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
import org.chombo.util.MedianStatsManager;
import org.chombo.util.NumericalAttrStatsManager;

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
			int intValue =  0;
			boolean status = false;
			try {
				intValue = Integer.parseInt(value);
				status = intValue >= attribute.getMin();
			} catch (Exception ex) {
			}
			return status;
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
			int intValue =  0;
			boolean status = false;
			try {
				intValue = Integer.parseInt(value);
				status = intValue <= attribute.getMax();
			} catch (Exception ex) {
			}
			return status;
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class StatsBasedRangeValidator extends Validator {
		private double min;
		private double max;
		
		public StatsBasedRangeValidator(String tag, int ordinal, AttributeSchema schema, Map<String, Object> validatorContext) {
			super(tag, ordinal, schema);
			NumericalAttrStatsManager statMan = (NumericalAttrStatsManager)validatorContext.get("stats");
			min = statMan.getMean(ordinal) - attribute.getMaxZscore() * statMan.getStdDev(ordinal);
			max = statMan.getMean(ordinal) + attribute.getMaxZscore() * statMan.getStdDev(ordinal);
		}

		@Override
		public boolean isValid(String value) {
			double dblValue =  0;
			boolean status = false;
			try {
				dblValue = Double.parseDouble(value);
				status = dblValue >= min && dblValue <= max;
			} catch (Exception ex) {
			}
			return status;
		}
	}
	/**
	 * @author pranab
	 *
	 */
	public static class RobustZscoreBasedRangeValidator extends Validator {
		private double min;
		private double max;
		
		public RobustZscoreBasedRangeValidator(String tag, int ordinal, AttributeSchema schema, Map<String, Object> validatorContext) {
			super(tag, ordinal, schema);
			MedianStatsManager statMan = (MedianStatsManager)validatorContext.get("stats");
			min = statMan.getMedian(ordinal) - attribute.getMaxZscore() * statMan.getMedAbsDivergence(ordinal);
			max = statMan.getMedian(ordinal) + attribute.getMaxZscore() * statMan.getMedAbsDivergence(ordinal);
		}

		@Override
		public boolean isValid(String value) {
			double dblValue =  0;
			boolean status = false;
			try {
				dblValue = Double.parseDouble(value);
				status = dblValue >= min && dblValue <= max;
			} catch (Exception ex) {
			}
			return status;
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
			double dblValue =  0;
			boolean status = false;
			try {
				dblValue = Double.parseDouble(value);
				status = dblValue >= attribute.getMin();
			} catch (Exception ex) {
			}
			return status;
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
			double dblValue =  0;
			boolean status = false;
			try {
				dblValue = Double.parseDouble(value);
				status = dblValue <= attribute.getMax();
			} catch (Exception ex) {
			}
			return status;
		}
	}
	
	
}
