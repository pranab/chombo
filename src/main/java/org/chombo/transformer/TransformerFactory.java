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

package org.chombo.transformer;

import org.chombo.util.ProcessorAttribute;

import com.typesafe.config.Config;

/**
 * @author pranab
 *
 */
public class TransformerFactory {
	public static final String LOWER_CASE_TRANSFORMER  = "lowerCase";
	public static final String UPPER_CASE_TRANSFORMER  = "upperCase";
	public static final String PATTERN_BASED_TRANSFORMER  = "patternBased";
	public static final String SEARCH_REPLACE_TRANSFORMER  = "searchReplace";
	public static final String KEY_VALUE_TRANSFORMER  = "keyValue";
	public static final String DEFAULT_VALUE_TRANSFORMER  = "defaultValue";
	public static final String ANOYNMIZER_TRANSFORMER  = "anoynmizer";
	public static final String UNIQUE_KEY_GENERATOR  = "uniqueKey";
	public static final String TRIM_TRANSFORMER  = "trim";
	public static final String LONG_POLYNOMIAL_TRANSFORMER  = "longPolynomial";
	public static final String DOUBLE_POLYNOMIAL_TRANSFORMER  = "doublePolynomial";
	public static final String LONG_CUSTOM_TRANSFORMER  = "longCustom";
	public static final String DOUBLE_CUSTOM_TRANSFORMER  = "longCustom";
	public static final String EPOCH_TIME_GENERATOR = "epochTimeGen";
	public static final String DATE_GENERATOR = "dateGen";
	public static final String DATE_FORMAT_TRANSFORMER = "dateFormat";
	public static final String NUM_DATA_DISCRETIZER = "discretizer";
	public static final String INT_ADD = "intAdd";
	public static final String INT_SUBTRACT = "intSubtract";
	public static final String INT_MULTIPLY = "intMultiply";
	public static final String INT_DIVIDE = "intDivide";
	public static final String CONST_GENERATOR = "constGenerator";
	public static final String GROUP_TRANFORMER = "groupTransformer";
	
	/**
	 * @param tag
	 * @param config
	 * @return
	 */
	public static AttributeTransformer createTransformer(String transformerTag,  ProcessorAttribute prAttr, Config config) {
		AttributeTransformer transformer = null;
		if (transformerTag.equals(LOWER_CASE_TRANSFORMER)) {
			transformer = new StringTransformer.LowerCaseTransformer(prAttr);
		} else if (transformerTag.equals(UPPER_CASE_TRANSFORMER)) {
			transformer = new StringTransformer.UpperCaseTransformer(prAttr);
		} else if (transformerTag.equals(PATTERN_BASED_TRANSFORMER)) {
			transformer = new StringTransformer.PatternBasedTransformer(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(SEARCH_REPLACE_TRANSFORMER)) {
			transformer = new StringTransformer.PatternBasedTransformer(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(KEY_VALUE_TRANSFORMER)) {
			transformer = new StringTransformer.KeyValueTransformer(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(DEFAULT_VALUE_TRANSFORMER)) {
			transformer = new StringTransformer.DefaultValueTransformer(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(ANOYNMIZER_TRANSFORMER)) {
			transformer = new StringTransformer.AnoynmizerTransformer(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(UNIQUE_KEY_GENERATOR)) {
			transformer = new StringTransformer.UniqueKeyGenerator(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(TRIM_TRANSFORMER)) {
			transformer = new StringTransformer.TrimTransformer(prAttr);
		} else if (transformerTag.equals(LONG_POLYNOMIAL_TRANSFORMER)) {
			transformer = new NumericTransformer.LongPolynomial(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(DOUBLE_POLYNOMIAL_TRANSFORMER)) {
			transformer = new NumericTransformer.DoublePolynomial(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(LONG_CUSTOM_TRANSFORMER)) {
			transformer = new NumericTransformer.LongCustom(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(DOUBLE_CUSTOM_TRANSFORMER)) {
			transformer = new NumericTransformer.DoubleCustom(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(EPOCH_TIME_GENERATOR)) {
			transformer = new DateTransformer.EpochTimeGenerator(prAttr);
		} else if (transformerTag.equals(DATE_GENERATOR)) {
			transformer = new DateTransformer.DateGenerator(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(DATE_FORMAT_TRANSFORMER)) {
			transformer = new DateTransformer.DateFormatTransformer(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(NUM_DATA_DISCRETIZER)) {
			transformer = new NumericTransformer.Discretizer(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(INT_ADD)) {
			transformer = new NumericTransformer.Adder(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(INT_SUBTRACT)) {
			transformer = new NumericTransformer.Subtracter(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(INT_MULTIPLY)) {
			transformer = new NumericTransformer.Multiplier(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(INT_DIVIDE)) {
			transformer = new NumericTransformer.Divider(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(CONST_GENERATOR)) {
			transformer = new StringTransformer.ConstantGenerator(prAttr, config.getConfig(transformerTag));
		} else if (transformerTag.equals(GROUP_TRANFORMER)) {
			transformer = new StringTransformer.GroupTransformer(prAttr, config.getConfig(transformerTag));
		} else {
			throw new IllegalArgumentException("invalid transformer");
		}
		
		return transformer;
	}
}
