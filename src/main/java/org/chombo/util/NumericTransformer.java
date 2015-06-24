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

package org.chombo.util;

import java.util.HashMap;
import java.util.Map;

import org.chombo.transformer.AttributeTransformer;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;


/**
 * @author pranab
 *
 */
public class NumericTransformer {

	/**
	 * polynomoial expression
	 * @author pranab
	 *
	 */
	public static class LongPolynomial  implements AttributeTransformer {
		private long a;
		private long b;
		private long c;
		private String[] transformed = new String[1];
		
		public LongPolynomial(long a, long b, long c) {
			super();
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
	public static class DoublePolynomial  implements AttributeTransformer {
		private double a;
		private double b;
		private double c;
		private String[] transformed = new String[1];
		
		public DoublePolynomial(double a, double b, double c) {
			super();
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
	public static abstract class Custom implements AttributeTransformer {
		private String script;
		private Map<String, Object> params = new HashMap<String, Object>();
		private Binding binding = new Binding();
		private String[] transformed = new String[1];
		
		
		public Custom(String script, Map<String, Object> params) {
			super();
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
	
}
