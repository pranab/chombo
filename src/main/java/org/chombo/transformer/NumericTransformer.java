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

import java.util.HashMap;
import java.util.Map;

import org.chombo.util.BaseAttribute;
import org.chombo.util.BasicUtils;
import org.chombo.util.BinaryCategoryCreator;
import org.chombo.util.ProcessorAttribute;
import org.chombo.util.Utility;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;


/**
 * @author pranab
 *
 */
public class NumericTransformer  {

	/**
	 * polynomoial expression
	 * @author pranab
	 *
	 */
	public static class LongPolynomial  extends AttributeTransformer {
		private long a;
		private long b;
		private long c;

		public LongPolynomial(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			this.a = config.getInt("a");
			this.b = config.getInt("b");
			this.c = config.getInt("c");
		}
		
		public LongPolynomial(long a, long b, long c) {
			super(1);
			this.a = a;
			this.b = b;
			this.c = c;
		}

		@Override
		public String[] tranform(String value) {
			long in = Long.parseLong(value);
			long out = a * in * in + b * in + c;
			transformed[0] =  "" + out;
			return transformed;
		}
	}
	
	/**
	 * polynomoial expression
	 * @author pranab
	 *
	 */
	public static class DoublePolynomial  extends AttributeTransformer {
		private double a;
		private double b;
		private double c;

		public DoublePolynomial(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			this.a = config.getInt("a");
			this.b = config.getInt("b");
			this.c = config.getInt("c");
		}
		
		public DoublePolynomial(double a, double b, double c) {
			super(1);
			this.a = a;
			this.b = b;
			this.c = c;
		}

		@Override
		public String[] tranform(String value) {
			double in = Double.parseDouble(value);
			double out = a * in * in + b * in + c;
			transformed[0] =  "" + out;
			return transformed;
		}
	}
	
	/**
	 * Custom groovy script
	 * @author pranab
	 *
	 */
	public static abstract class Custom extends AttributeTransformer {
		private String script;
		private Map<String, Object> params = new HashMap<String, Object>();
		private Binding binding = new Binding();
		
		public Custom(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			this.script = config.getString("script");
			for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
				Object value = entry.getValue().unwrapped();
				binding.setVariable(entry.getKey(), value);
			}
		}
		
		public Custom(String script, Map<String, Object> params) {
			super(1);
			this.script = script;
			this.params = params;
			for (String name : params.keySet()) {
				binding.setVariable(name, params.get(name));
			}
		}


		@Override
		public String[] tranform(String value) {
			Object in = getFieldValue(value);
			binding.setVariable("field", in);
			GroovyShell shell = new GroovyShell(binding);
			Object out = shell.evaluate(script);
			transformed[0] =  getOutput(out);
			return transformed;
		}
		
		protected abstract Object getFieldValue(String value);
		
		protected abstract String getOutput(Object out);
		
	}
	
	
	/**
	 * @author pranab
	 *
	 */
	public static class LongCustom extends Custom {
		
		public LongCustom(ProcessorAttribute prAttr, Config config) {
			super(prAttr, config);
		}		
		
		public LongCustom(String script, Map<String, Object> params) {
			super(script,  params);
		}

		protected  Object getFieldValue(String value) {
			Long in = null;
			try {
				in = Long.parseLong(value);
			} catch (Exception ex) {
				throw new IllegalArgumentException("long input expected");
			}
			return in;
		}
		
		protected  String getOutput(Object out) {
			String ret = null;
			if (out instanceof Long || out instanceof Integer) {
				ret = "" + out;
			} else {
				throw new IllegalArgumentException("int or long output expected");
			}
			return ret;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class DoubleCustom extends Custom {

		public DoubleCustom(ProcessorAttribute prAttr, Config config) {
			super(prAttr, config);
		}		
		public DoubleCustom(String script, Map<String, Object> params) {
			super(script,  params);
		}

		protected  Object getFieldValue(String value) {
			Double in = null;
			try {
				in = Double.parseDouble(value);
			} catch (Exception ex) {
				throw new IllegalArgumentException("double input expected");
			}
			return in;
		}
		
		protected  String getOutput(Object out) {
			String ret = null;
			if (out instanceof Double || out instanceof Float) {
				ret = "" + out;
			} else {
				throw new IllegalArgumentException("double or float output expected");
			}
			return ret;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class Discretizer  extends AttributeTransformer {
		private double bucketWidth;
		private String dataType;
		
		public Discretizer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			bucketWidth = prAttr.getBuckeWidth();
			dataType = prAttr.getDataType();
		}
		
		@Override
		public String[] tranform(String value) {
			int bucket = 0;
			if (dataType.equals(BaseAttribute.DATA_TYPE_INT)) {
				int iVal = Integer.parseInt(value);
				bucket = (int)(iVal / bucketWidth);
			} else if (dataType.equals(BaseAttribute.DATA_TYPE_LONG)) {
				long  lVal = Long.parseLong(value);
				bucket = (int)(lVal / bucketWidth);
			} else if (dataType.equals(BaseAttribute.DATA_TYPE_DOUBLE)) {
				double  dVal = Double.parseDouble(value);
				bucket = (int)(dVal / bucketWidth);
			} else {
				throw new IllegalArgumentException("only numeric data can be discretized");
			}
			transformed[0] = "" + bucket;;
			return transformed;
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class BinaryCreator  extends AttributeTransformer {
		private BinaryCategoryCreator binaryCategoryCreator;
		private String dataType;
		
		public BinaryCreator(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			dataType = prAttr.getDataType();
			
			long threshold = config.getLong("threshold");
			String lowerToken = config.getString("lowerToken");
			String upperToken = config.getString("upperToken");
			binaryCategoryCreator = new BinaryCategoryCreator(threshold, lowerToken, upperToken);
		}
		
		@Override
		public String[] tranform(String value) {
			String token = null;
			if (dataType.equals(BaseAttribute.DATA_TYPE_INT) || dataType.equals(BaseAttribute.DATA_TYPE_LONG)) {
				long  lVal = Long.parseLong(value);
				token = binaryCategoryCreator.findToken(lVal);
			}  else {
				throw new IllegalArgumentException("only numeric integer data can be discretized");
			}
			transformed[0] = token;
			return transformed;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static abstract class Operator  extends AttributeTransformer {
		private  boolean isInt;
		protected int iOperand;
		protected double dOperand;
		private int precision;
		
		public Operator(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			isInt = prAttr.isInteger();
			if (isInt) {
				iOperand = config.getInt("intOperand");
			} else {
				dOperand = config.getDouble("dblOperand");
				precision = config.getInt("precision");
			}
		}

		@Override
		public String[] tranform(String value) {
			if (isInt) {
				int iValue = Integer.parseInt(value);
				iValue = operate(iValue);
				transformed[0] = "" + iValue;
			} else {
				double dValue = Double.parseDouble(value);
				dValue = operate(dValue);
				transformed[0] =  Utility.formatDouble(dValue, precision);
			}
			return transformed;
		}
		
		protected abstract int operate(int value);

		protected abstract double operate(double value);

	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class Adder extends Operator {
		public Adder(ProcessorAttribute prAttr, Config config) {
			super(prAttr, config);
		}

		@Override
		protected int operate(int value) {
			return value + iOperand;
		}

		@Override
		protected double operate(double value) {
			return value + dOperand;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class Subtracter  extends Operator {
		public Subtracter(ProcessorAttribute prAttr, Config config) {
			super(prAttr, config);
		}

		@Override
		protected int operate(int value) {
			return value - iOperand;
		}

		@Override
		protected double operate(double value) {
			return value - dOperand;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class Multiplier  extends Operator {
		public Multiplier(ProcessorAttribute prAttr, Config config) {
			super(prAttr, config);
		}

		@Override
		protected int operate(int value) {
			return value * iOperand;
		}

		@Override
		protected double operate(double value) {
			return value * dOperand;
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class Divider  extends Operator {
		public Divider(ProcessorAttribute prAttr, Config config) {
			super(prAttr, config);
		}

		@Override
		protected int operate(int value) {
			return value / iOperand;
		}

		@Override
		protected double operate(double value) {
			return value / dOperand;
		}
	}
	
	
	/**
	 * Binary operation between 2 fields
	 * @author pranab
	 *
	 */
	public static class BinaryArithmeticOperator  extends AttributeTransformer {
		private String operator;
		private int firstOperandFieldOrdinal;
		private int secondOperandFieldOrdinal;
		private String outputDataType;
		private int outputPrecision;
		private String fieldDelim;
		
		public BinaryArithmeticOperator(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			operator = config.getString("operator");
			firstOperandFieldOrdinal = config.getInt("firstOperandFieldOrdinal");
			secondOperandFieldOrdinal = config.getInt("secondOperandFieldOrdinal");
			fieldDelim = config.getString("fieldDelim");
			outputDataType = config.getString("outputDataType");
			outputPrecision = config.getInt("outputPrecision");
		}
		
		@Override
		public String[] tranform(String value) {
			String[] items = value.split(fieldDelim, -1);
			double firstVal = Double.parseDouble(items[firstOperandFieldOrdinal]);
			double secondVal = Double.parseDouble(items[secondOperandFieldOrdinal]);
			double result = 0;
			if (operator.equals("+")) {
				result = firstVal + secondVal;
			} else if (operator.equals("-")) {
				result = firstVal - secondVal;
			} else if (operator.equals("*")) {
				result = firstVal * secondVal;
			} else if (operator.equals("/")) {
				result = firstVal / secondVal;
			} else {
				throw new IllegalStateException("invalid operator");
			}
			
			if (outputDataType.equals(BaseAttribute.DATA_TYPE_INT) || 
					outputDataType.equals(BaseAttribute.DATA_TYPE_LONG)) {
				transformed[0] = "" + BasicUtils.roundToInt(result);
			} else if (outputDataType.equals(BaseAttribute.DATA_TYPE_DOUBLE) || 
					outputDataType.equals(BaseAttribute.DATA_TYPE_FLOAT)) {
				transformed[0] = "" + BasicUtils.formatDouble(result, outputPrecision);
			} else {
				throw new IllegalStateException("invalid data type");
			}
			return transformed;
		}
	}

	/**
	 * Binary operation between a field and a constant
	 * @author pranab
	 *
	 */
	public static class BinaryConstOperator  extends AttributeTransformer {
		private String operator;
		private String outputDataType;
		private int outputPrecision;
		private double constant;
		
		public BinaryConstOperator(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			operator = config.getString("operator");
			outputDataType = config.getString("outputDataType");
			outputPrecision = config.getInt("outputPrecision");
			constant = config.getDouble("constant");
		}
		
		@Override
		public String[] tranform(String value) {
			double dVal = Double.parseDouble(value);
			double result = 0;
			if (operator.equals("+")) {
				result = dVal + constant;
			} else if (operator.equals("-")) {
				result = dVal - constant;
			} else if (operator.equals("*")) {
				result = dVal * constant;
			} else if (operator.equals("/")) {
				result = dVal / constant;
			} else {
				throw new IllegalStateException("invalid operator");
			}
			
			if (outputDataType.equals(BaseAttribute.DATA_TYPE_INT) || 
					outputDataType.equals(BaseAttribute.DATA_TYPE_LONG)) {
				transformed[0] = "" + BasicUtils.roundToInt(result);
			} else if (outputDataType.equals(BaseAttribute.DATA_TYPE_DOUBLE) || 
					outputDataType.equals(BaseAttribute.DATA_TYPE_FLOAT)) {
				transformed[0] = "" + BasicUtils.formatDouble(result, outputPrecision);
			} else {
				throw new IllegalStateException("invalid data type");
			}
			return transformed;
		}
	}
	
	/**
	 * Rounds off floating to intgere
	 * @author pranab
	 *
	 */
	public static class IntegerRoundOff  extends AttributeTransformer {
		
		public IntegerRoundOff(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
		}
		
		@Override
		public String[] tranform(String value) {
			long roudedOffVal = Math.round(Double.parseDouble(value));
			transformed[0] = "" + roudedOffVal;
			return transformed;
		}
	}

	/**
	 * Rounds off floating to floating
	 * @author pranab
	 *
	 */
	public static class FloatingRoundOff  extends AttributeTransformer {
		private int outputPrecision;
		
		public FloatingRoundOff(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			outputPrecision = config.getInt("outputPrecision");
		}
		
		@Override
		public String[] tranform(String value) {
			double dVal = Double.parseDouble(value);
			transformed[0] = BasicUtils.formatDouble(dVal, outputPrecision);
			return transformed;
		}
	}

}
