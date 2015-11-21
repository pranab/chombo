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
import org.chombo.util.ProcessorAttribute;

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
	public static final String ZCORE_BASED_RANGE_VALIDATOR = "zscoreBasedRange";
	public static final String ROBUST_ZCORE_BASED_RANGE_VALIDATOR = "robustZscoreBasedRange";
	
	private static Map<String,String> custValidatorClasses = new HashMap<String,String>();
	
	/**
	 * @param tag
	 * @param ordinal
	 * @param schema
	 * @return
	 */
	public static Validator create(String validatorType, ProcessorAttribute prAttr) {
		return create(validatorType,   prAttr, null);
	}
	
	/**
	 * @param tag
	 * @param ordinal
	 * @param schema
	 * @return
	 */
	public static Validator create(String validatorType,  ProcessorAttribute prAttr, 
			Map<String, Object> validatorContext) {
		Validator validator = null;
		//Attribute attribute = schema.findAttributeByOrdinal(ordinal);
		
		if (validatorType.equals(MIN_VALIDATOR)) {
			if (prAttr.isInteger()) {
				validator = new  NumericalValidator.IntMinValidator(validatorType,  prAttr);
			} else if (prAttr.isDouble()) {
				validator = new  NumericalValidator.DoubleMinValidator(validatorType, prAttr);
			} else if (prAttr.isString()) {
				validator = new  StringValidator.MinValidator(validatorType, prAttr);
			}
		} else if (validatorType.equals(MAX_VALIDATOR)) {
			if (prAttr.isInteger()) {
				validator = new  NumericalValidator.IntMaxValidator(validatorType, prAttr);
			} else if (prAttr.isDouble()) {
				validator = new  NumericalValidator.DoubleMaxValidator(validatorType, prAttr);
			} else if (prAttr.isString()) {
				validator = new  StringValidator.MaxValidator(validatorType, prAttr);
			}
		} else if (validatorType.equals(MIN_LENGTH_VALIDATOR)) {
			if (prAttr.isString()) {
				validator = new  StringValidator.MinLengthValidator(validatorType, prAttr);
			}
		} else if (validatorType.equals(MAX_LENGTH_VALIDATOR)) {
			if (prAttr.isString()) {
				validator = new  StringValidator.MaxLengthValidator(validatorType, prAttr);
			}
		} else if (validatorType.equals(EXACT_LENGTH_VALIDATOR)) {
			if (prAttr.isString()) {
				validator = new  StringValidator.LengthValidator(validatorType, prAttr);
			}
		} else if (validatorType.equals(NOT_MISSING_VALIDATOR)) {
			validator = new  GenericValidator.NotMissingValidator(validatorType, prAttr);
		} else if (validatorType.equals(PATTERN_VALIDATOR)) {
			if (prAttr.isString()) {
				validator = new  StringValidator.PatternValidator(validatorType, prAttr);
			}
		} else if (validatorType.equals(MEMEBERSHIP_VALIDATOR)) {
			if (prAttr.isCategorical()) {
				validator = new CategoricalValidator.MembershipValidator(validatorType, prAttr);
			}
		} else if (validatorType.equals(ENSURE_INT_VALIDATOR)) {
				validator = new GenericValidator.EnsureIntValidator(validatorType, prAttr);
		} else if (validatorType.equals(ENSURE_LONG_VALIDATOR)) {
			validator = new GenericValidator.EnsureLongValidator(validatorType, prAttr);
		} else if (validatorType.equals(ENSURE_DOUBLE_VALIDATOR)) {
			validator = new GenericValidator.EnsureDoubleValidator(validatorType, prAttr);
		} else if (validatorType.equals( ZCORE_BASED_RANGE_VALIDATOR)) {
				validator = new  NumericalValidator.StatsBasedRangeValidator(validatorType, prAttr, validatorContext);
		} else if (validatorType.equals( ROBUST_ZCORE_BASED_RANGE_VALIDATOR)) {
			validator = new  NumericalValidator.RobustZscoreBasedRangeValidator(validatorType, prAttr, validatorContext);
		} else {
			//custor validator
			validator = createCustomValidator(validatorType, prAttr);
			
			if (null == validator) {
				throw new IllegalArgumentException("invalid val;idator type   validator:" + validatorType);
			}
		}
		
		if (null == validator) {
			throw new IllegalArgumentException(" validator and attribute data type is not compatible validator: " + 
					validatorType + " ordinal:" + prAttr.getOrdinal() + " data type:" + prAttr.getDataType() );
		}
		
		return validator;
	}

	/**
	 * @param validatorType
	 * @param ordinal
	 * @param schema
	 * @return
	 */
	private static Validator  createCustomValidator(String validatorType, ProcessorAttribute prAttr) {
		Validator validator = null;
		String validatorClass = custValidatorClasses.get(validatorType);
		if (null != validatorClass) {
			try {
				Class<?> clazz = Class.forName(validatorClass);
				Constructor<?> ctor = clazz.getConstructor(String.class, prAttr.getClass());
				validator = (Validator)(ctor.newInstance(new Object[] { validatorType, prAttr}));
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
	
	/**
	 * @param validatotType
	 * @return
	 */
	public static boolean isCustomValidator(String validatotType) {
		return custValidatorClasses.containsKey(validatotType);
	}
	
	/**
	 * @param validatotType
	 * @return
	 */
	public static boolean isStatBasedValidator(String validatotType) {
		return validatotType.equals(ZCORE_BASED_RANGE_VALIDATOR) || 
				validatotType.equals(ROBUST_ZCORE_BASED_RANGE_VALIDATOR);
	}
}
