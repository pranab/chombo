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
import java.util.List;
import java.util.Map;

import org.chombo.util.ProcessorAttribute;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

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
	private static Map<String,Validator> custValidators = new HashMap<String,Validator>();
	private static CustomValidatorFactory customValidatorFactory;
	
	/**
	 * @param customValidFactoryClass
	 * @param validatorConfig
	 */
	public static void initialize(String customValidFactoryClass, Config validatorConfig) {
		if (null != customValidFactoryClass) {
			Class<?>factoryCls = null;
			try {
				factoryCls = Class.forName(customValidFactoryClass);
				customValidatorFactory = (CustomValidatorFactory)factoryCls.newInstance();
			} catch (ClassNotFoundException cne) {
				throw new IllegalArgumentException("custom validation factory class could not be created " + cne.getMessage());
			} catch (InstantiationException ie) {
				throw new IllegalStateException("custom validation factory instance could not be created " + ie.getMessage());
			} catch (IllegalAccessException iae) {
				throw new IllegalStateException("custom  validation factory instance could not be created with access issue " + iae.getMessage());
			}
		}
		
		//custom validaor classes
		if (null == customValidatorFactory && null != validatorConfig) {
			if (validatorConfig.hasPath("validators.customValidators")) {
				List <? extends Config> customValidConfigs = validatorConfig.getConfigList("validators.customValidators");
				for (Config customValidConfig : customValidConfigs ) {
					custValidatorClasses.put("custom.validator.class." + customValidConfig.getString("tag"), customValidConfig.getString("class"));
				}
			}
		}
		
	}

	/**
	 * @param tag
	 * @param ordinal
	 * @param schema
	 * @return
	 */
	public static Validator create(String validatorType, ProcessorAttribute prAttr) {
		return create(validatorType,   prAttr, null, null);
	}
	
	/**
	 * @param tag
	 * @param ordinal
	 * @param schema
	 * @return
	 */
	public static Validator create(String validatorType, ProcessorAttribute prAttr, Map<String, Object> validatorContext) {
		return create(validatorType,   prAttr, validatorContext, null);
	}

	/**
	 * @param tag
	 * @param ordinal
	 * @param schema
	 * @return
	 */
	public static Validator create(String validatorType, ProcessorAttribute prAttr, Config validatorConfig) {
		return create(validatorType,   prAttr, null, validatorConfig);
	}

	/**
	 * @param validatorType
	 * @param prAttr
	 * @param validatorContext
	 * @param validatorConfig
	 * @return
	 */
	public static Validator create(String validatorType,  ProcessorAttribute prAttr, 
			Map<String, Object> validatorContext, Config validatorConfig) {
		Validator validator = null;
		Config valConfig =  getValidatorConfig(validatorConfig ,validatorType, prAttr);
		
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
		} else if (null != valConfig){
			//custom validator with configured validator class names
			validator = createCustomValidator(validatorType, prAttr,  valConfig);
			
			//custom validator factory
			if (null == validator && null != customValidatorFactory) {
				validator = customValidatorFactory.createValidator(validatorType, prAttr,  valConfig);
			}

			if (null == validator) {
				throw new IllegalArgumentException("invalid val;idator type   validator:" + validatorType +  " ordinal:" + 
						prAttr.getOrdinal() + " data type:" + prAttr.getDataType());
			}
		}
		
		return validator;
	}

	/**
	 * @param validatorType
	 * @param prAttr
	 * @param validatorConfig
	 * @return
	 */
	private static Validator  createCustomValidator(String validatorType, ProcessorAttribute prAttr,   Config validatorConfig) {
		Validator validator = custValidators.get(validatorType);
		
		if (null == validator) {
			String validatorClass = custValidatorClasses.get("custom.validator.class." + validatorType);
			if (null != validatorClass) {
				try {
					Class<?> clazz = Class.forName(validatorClass);
					Constructor<?> ctor = clazz.getConstructor(String.class, prAttr.getClass(), Config.class);
					validator = (Validator)(ctor.newInstance(new Object[] { validatorType, prAttr, validatorConfig}));
					custValidators.put(validatorType, validator);
				} catch (Exception ex) {
					throw new IllegalArgumentException("could not create dynamic validator object for " + validatorType + " " +  ex.getMessage());
				}
			}
		}
		return validator;
	}
	
    /**
     * @param transformerConfig
     * @param validatorTag
     * @param prAttr
     * @return
     */
    public static Config getValidatorConfig(Config transformerConfig ,String validatorTag, ProcessorAttribute prAttr) {
	if(null == transformerConfig)
		return null;

	if(!transformerConfig.hasPath("validators." + validatorTag))
		return null;

    	Config valConfig = transformerConfig.getConfig("validators." + validatorTag);
    	Config config = null;
    	try {
    		//attribute specific config
    		config = null != valConfig ?  valConfig.getConfig(prAttr.getName()) : null;
    	} catch ( ConfigException.Missing ex) {
    	}
    	
    	return null != config ? config :  valConfig;
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
