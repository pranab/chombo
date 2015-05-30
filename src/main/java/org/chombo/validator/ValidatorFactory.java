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

import org.chombo.util.Attribute;
import org.chombo.util.AttributeSchema;

/**
 * @author pranab
 *
 */
public class ValidatorFactory {
	public static final String MIN_VALIDATOR = "minValidator";
	public static final String MAX_VALIDATOR = "maxValidator";
	public static final String NOT_MISSING_VALIDATOR = "notMissingValidator";
	public static final String PATTERN_VALIDATOR = "patternValidator";
	public static final String MEMEBERSHIP_VALIDATOR = "membershipValidator";
	
	
	/**
	 * @param tag
	 * @param ordinal
	 * @param schema
	 * @return
	 */
	public static Validator create(String validatorType, int ordinal, AttributeSchema schema) {
		Validator validator = null;
		Attribute attribute = schema.findAttributeByOrdinal(ordinal);
		
		if (validatorType.equals(MIN_VALIDATOR)) {
			if (attribute.isInteger()) {
				validator = new  NumericalValidator.IntMinValidator(validatorType, ordinal, schema);
			} else if (attribute.isDouble()) {
				validator = new  NumericalValidator.DoubleMinValidator(validatorType, ordinal, schema);
			} else if (attribute.isString()) {
				validator = new  StringValidator.MinLengthValidator(validatorType, ordinal, schema);
			}
		} else if (validatorType.equals(MAX_VALIDATOR)) {
			if (attribute.isInteger()) {
				validator = new  NumericalValidator.IntMaxValidator(validatorType, ordinal, schema);
			} else if (attribute.isDouble()) {
				validator = new  NumericalValidator.DoubleMaxValidator(validatorType, ordinal, schema);
			} else if (attribute.isString()) {
				validator = new  StringValidator.MaxLengthValidator(validatorType, ordinal, schema);
			}
		} else if (validatorType.equals(NOT_MISSING_VALIDATOR)) {
			validator = new  GenericValidator.NotMissingValidator(validatorType, ordinal, schema);
		} else if (validatorType.equals(PATTERN_VALIDATOR)) {
			if (attribute.isString()) {
				validator = new  StringValidator.PatternValidator(validatorType, ordinal, schema);
			}
		} else if (validatorType.equals(MEMEBERSHIP_VALIDATOR)) {
			if (attribute.isCategorical()) {
				validator = new CategoricalValidator.MembershipValidator(validatorType, ordinal, schema);
			}
		} else {
			throw new IllegalArgumentException("invalid val;idator type   validator:" + validatorType);
		}
		
		if (null == validator) {
			throw new IllegalArgumentException("validator and attribute data type is not compatible validator: " + 
					validatorType + " ordinal:" + ordinal );
		}
		
		return validator;
	}
	
}
