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

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.chombo.util.Attribute;
import org.chombo.util.AttributeSchema;

/**
 * @author pranab
 *
 */
public class ValidatorFactory {
	public static final String MIN_VALIDATOR = "min";
	public static final String MAX_VALIDATOR = "max";
	public static final String MIN_LENGTH_VALIDATOR = "minLength";
	public static final String MAX_LENGTH_VALIDATOR = "maxLength";
	public static final String EXACT_LENGTH_VALIDATOR = "exactLength";
	public static final String NOT_MISSING_VALIDATOR = "notMissing";
	public static final String PATTERN_VALIDATOR = "pattern";
	public static final String MEMEBERSHIP_VALIDATOR = "membership";
	public static final String ENSURE_INT_VALIDATOR = "ensureInt";
	public static final String ENSURE_LONG_VALIDATOR = "ensureLong";
	public static final String ENSURE_DOUBLE_VALIDATOR = "ensureDouble";
	public static final String STATS_BASED_RANGE_VALIDATOR = "statBasedRange";
	
	private static Map<String,String> custValidatorClasses = new HashMap<String,String>();
	
	/**
	 * @param tag
	 * @param ordinal
	 * @param schema
	 * @return
	 */
	public static Validator create(String validatorType, int ordinal, AttributeSchema<Attribute> schema) {
		return create(validatorType, ordinal, schema, null);
	}
	
	/**
	 * @param tag
	 * @param ordinal
	 * @param schema
	 * @return
	 */
	public static Validator create(String validatorType, int ordinal, AttributeSchema<Attribute> schema, 
			Map<String, Object> validatorContext) {
		Validator validator = null;
		Attribute attribute = schema.findAttributeByOrdinal(ordinal);
		
		if (validatorType.equals(MIN_VALIDATOR)) {
			if (attribute.isInteger()) {
				validator = new  NumericalValidator.IntMinValidator(validatorType, ordinal, schema);
			} else if (attribute.isDouble()) {
				validator = new  NumericalValidator.DoubleMinValidator(validatorType, ordinal, schema);
			} else if (attribute.isString()) {
				validator = new  StringValidator.MinValidator(validatorType, ordinal, schema);
			}
		} else if (validatorType.equals(MAX_VALIDATOR)) {
			if (attribute.isInteger()) {
				validator = new  NumericalValidator.IntMaxValidator(validatorType, ordinal, schema);
			} else if (attribute.isDouble()) {
				validator = new  NumericalValidator.DoubleMaxValidator(validatorType, ordinal, schema);
			} else if (attribute.isString()) {
				validator = new  StringValidator.MaxValidator(validatorType, ordinal, schema);
			}
		} else if (validatorType.equals(MIN_LENGTH_VALIDATOR)) {
			if (attribute.isString()) {
				validator = new  StringValidator.MinLengthValidator(validatorType, ordinal, schema);
			}
		} else if (validatorType.equals(MAX_LENGTH_VALIDATOR)) {
			if (attribute.isString()) {
				validator = new  StringValidator.MaxLengthValidator(validatorType, ordinal, schema);
			}
		} else if (validatorType.equals(EXACT_LENGTH_VALIDATOR)) {
			if (attribute.isString()) {
				validator = new  StringValidator.LengthValidator(validatorType, ordinal, schema);
			}
		} else if (validatorType.equals(NOT_MISSING_VALIDATOR)) {
			System.out.println("validator type:" + validatorType + " ordinal:" + ordinal );
			validator = new  GenericValidator.NotMissingValidator(validatorType, ordinal, schema);
		} else if (validatorType.equals(PATTERN_VALIDATOR)) {
			if (attribute.isString()) {
				validator = new  StringValidator.PatternValidator(validatorType, ordinal, schema);
			}
		} else if (validatorType.equals(MEMEBERSHIP_VALIDATOR)) {
			if (attribute.isCategorical()) {
				validator = new CategoricalValidator.MembershipValidator(validatorType, ordinal, schema);
			}
		} else if (validatorType.equals(ENSURE_INT_VALIDATOR)) {
				validator = new GenericValidator.EnsureIntValidator(validatorType, ordinal, schema);
		} else if (validatorType.equals(ENSURE_LONG_VALIDATOR)) {
			validator = new GenericValidator.EnsureLongValidator(validatorType, ordinal, schema);
		} else if (validatorType.equals(ENSURE_DOUBLE_VALIDATOR)) {
			validator = new GenericValidator.EnsureDoubleValidator(validatorType, ordinal, schema);
		} else if (validatorType.equals(STATS_BASED_RANGE_VALIDATOR)) {
			if (attribute.isInteger()) {
				validator = new  NumericalValidator.StatsBasedIntRangeValidator(validatorType, ordinal, schema, validatorContext);
			} else if (attribute.isDouble()) {
				validator = new  NumericalValidator.StatsBasedDoubleRangeValidator(validatorType, ordinal, schema, validatorContext);
			} 
		} else {
			//custor validator
			validator = createCustomValidator(validatorType, ordinal,  schema);
			
			if (null == validator) {
				throw new IllegalArgumentException("invalid val;idator type   validator:" + validatorType);
			}
		}
		
		if (null == validator) {
			throw new IllegalArgumentException(" validator and attribute data type is not compatible validator: " + 
					validatorType + " ordinal:" + ordinal + " data type:" + attribute.getDataType() );
		}
		
		return validator;
	}

	/**
	 * @param validatorType
	 * @param ordinal
	 * @param schema
	 * @return
	 */
	private static Validator  createCustomValidator(String validatorType, int ordinal, AttributeSchema<Attribute> schema) {
		Validator validator = null;
		String validatorClass = custValidatorClasses.get(validatorType);
		if (null != validatorClass) {
			try {
				Class<?> clazz = Class.forName(validatorClass);
				Constructor<?> ctor = clazz.getConstructor(String.class, Integer.class, schema.getClass());
				validator = (Validator)(ctor.newInstance(new Object[] { validatorType, ordinal, schema }));
			} catch (Exception ex) {
				throw new IllegalArgumentException("could not create dynamic validator object for " + validatorType + " " +  ex.getMessage());
			}
		}
		return validator;
	}
	
	/**
	 * @param custValidatorClasses
	 */
	public static void setCustValidatorClasses(Map<String, String> custValidatorClasses) {
		ValidatorFactory.custValidatorClasses = custValidatorClasses;
	}
	
}
