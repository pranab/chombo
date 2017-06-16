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

import org.chombo.stats.MedianStatsManager;
import org.chombo.stats.NumericalAttrStatsManager;
import org.chombo.util.AttributeSchema;
import org.chombo.util.ProcessorAttribute;
import org.chombo.util.Utility;

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

		public IntMinValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			int intValue =  0;
			boolean status = false;
			try {
				intValue = Integer.parseInt(value);
				status = intValue >= prAttr.getMin();
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

		public IntMaxValidator(String tag,  ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			int intValue =  0;
			boolean status = false;
			try {
				intValue = Integer.parseInt(value);
				status = intValue <= prAttr.getMax();
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
		
		public StatsBasedRangeValidator(String tag,  ProcessorAttribute prAttr, Map<String, Object> validatorContext) {
			super(tag,   prAttr);
			int ordinal = prAttr.getOrdinal();
			NumericalAttrStatsManager statMan = (NumericalAttrStatsManager)validatorContext.get("stats");
			min = statMan.getMean(ordinal) - prAttr.getMaxZscore() * statMan.getStdDev(ordinal);
			max = statMan.getMean(ordinal) + prAttr.getMaxZscore() * statMan.getStdDev(ordinal);
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
		private Map<String, Object> validatorContext;
		
		public RobustZscoreBasedRangeValidator(String tag, ProcessorAttribute prAttr, Map<String, Object> validatorContext) {
			super(tag,  prAttr);
			this.validatorContext = validatorContext;
		}

		@Override
		public boolean isValid(String value) {
			double dblValue =  0;
			boolean status = false;
			try {
				MedianStatsManager statMan = (MedianStatsManager)validatorContext.get("stats");
				String[] items = value.split(fieldDelim);
				int[] idOrdinals = statMan.getIdOrdinals();
				int ordinal = prAttr.getOrdinal();
				if (null != idOrdinals) {
					String compKey = Utility.join(items, idOrdinals, fieldDelim);
					min = statMan.getKeyedMedian(compKey, ordinal) - 
								prAttr.getMaxZscore() * statMan.getKeyedMedAbsDivergence(compKey, ordinal);
					max = statMan.getKeyedMedian(compKey, ordinal) +
								prAttr.getMaxZscore() * statMan.getKeyedMedAbsDivergence(compKey, ordinal);
				} else  {
						min = statMan.getMedian(ordinal) - prAttr.getMaxZscore() * statMan.getMedAbsDivergence(ordinal);
						max = statMan.getMedian(ordinal) + prAttr.getMaxZscore() * statMan.getMedAbsDivergence(ordinal);
				}
				dblValue = Double.parseDouble(items[ordinal]);
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

		public DoubleMinValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			double dblValue =  0;
			boolean status = false;
			try {
				dblValue = Double.parseDouble(value);
				status = dblValue >= prAttr.getMin();
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

		public DoubleMaxValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			double dblValue =  0;
			boolean status = false;
			try {
				dblValue = Double.parseDouble(value);
				status = dblValue <= prAttr.getMax();
			} catch (Exception ex) {
			}
			return status;
		}
	}
	
	
}
