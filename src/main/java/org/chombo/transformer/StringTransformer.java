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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.chombo.transformer.NumericTransformer.Custom;
import org.chombo.util.ProcessorAttribute;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

/**
 * @author pranab
 *
 */
public class StringTransformer {
	
	/**
	 * @author pranab
	 *
	 */
	public static class LowerCaseTransformer extends AttributeTransformer  {

		public LowerCaseTransformer(ProcessorAttribute prAttr) {
			super(prAttr.getTargetFieldOrdinals().length);
		}

		public LowerCaseTransformer() {
			super(1);
		}

		@Override
		public String[] tranform(String value) {
			transformed[0] =  value.toLowerCase();
			return transformed;
		}
		
	}

	/**
	 * @author pranab
	 *
	 */
	public static class UpperCaseTransformer extends AttributeTransformer  {
		
		public UpperCaseTransformer(ProcessorAttribute prAttr) {
			super(prAttr.getTargetFieldOrdinals().length);
		}
		
		public UpperCaseTransformer() {
			super(1);
		}

		@Override
		public String[] tranform(String value) {
			transformed[0] =  value.toUpperCase();
			return transformed;
		}
		
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class PatternBasedTransformer extends AttributeTransformer {
		private Pattern pattern;
		private Matcher matcher;
		private boolean failOnMissingGroup;
		
		public PatternBasedTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			pattern = Pattern.compile(config.getString("regEx"));
			failOnMissingGroup = config.getBoolean("failOnMissingGroup");
		}

		public PatternBasedTransformer(int numTransAttributes, String regEx, boolean failOnMissingGroup) {
			super(numTransAttributes);
			pattern = Pattern.compile(regEx);
			this.failOnMissingGroup =  failOnMissingGroup;
		}

		@Override
		public String[] tranform(String value) {
			matcher = pattern.matcher(value);
			if (matcher.matches()) {
				for (int i = 0; i < transformed.length; ++i) {
			        String extracted = matcher.group(i+1);
			        if(extracted != null) {
			        	transformed[i] = extracted;
			        } else {
			        	if (failOnMissingGroup) {
			        		throw new IllegalArgumentException("mtaching failed for a group in  pattern based transformer");
			        	} else {
			        		transformed[i] = "";
			        	}
			        }
			    }
			} else {
				throw new IllegalArgumentException("mtaching failed for pattern based transformer");
			}
			return transformed;
		}
		
	}

	/**
	 * @author pranab
	 *
	 */
	public static class SearchReplaceTransformer extends AttributeTransformer {
		private String regEx;
		private String replacement;
		private boolean replaceAll;
		
		public SearchReplaceTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			regEx  = config.getString("regEx");
			replacement = config.getString("replacement");
			replaceAll = config.getBoolean("replaceAll");
		}
		
		public SearchReplaceTransformer(int numTransAttributes, String regEx, String replacement, boolean replaceAll) {
			super(numTransAttributes);
			this.regEx  = regEx;
			this.replacement = replacement;
			this.replaceAll = replaceAll;
		}

		@Override
		public String[] tranform(String value) {
			if (replaceAll) {
				transformed[0] = value.replaceAll(regEx, replacement);
			} else {
				transformed[0] = value.replaceFirst(regEx, replacement);
			}
			return transformed;
		}
		
	}

	/**
	 * @author pranab
	 *
	 */
	public static class KeyValueTransformer extends AttributeTransformer {
		private Config config;
		private Map<String, String>  kayValues;
		
		public KeyValueTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			this.config = config;
		}

		public KeyValueTransformer( Map<String, String>  kayValues) {
			super(1);
			this.kayValues = kayValues;
		}

		@Override
		public String[] tranform(String value) {
			String newValue = null;
			if (null != config) {
				newValue = config.getString(value);
			} else {
				newValue = kayValues.get(value);
			}
			
			transformed[0] = null != newValue ? newValue  :  value;
			return transformed;
		}		
	}	
	
	/**
	 * @author pranab
	 *
	 */
	public static class DefaultValueTransformer extends AttributeTransformer {
		private String defaultValue;
		
		public DefaultValueTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			defaultValue  = config.getString("defaultValue");
		}

		public DefaultValueTransformer( String defaultValue) {
			super(1);
			this.defaultValue  = defaultValue;
		}

		@Override
		public String[] tranform(String value) {
			if (value.isEmpty()) {
				transformed[0] = defaultValue;
			} else {
				transformed[0] = value;
			}
			return transformed;
		}
	}	
	
	/**
	 * @author pranab
	 *
	 */
	public static class ForcedReplaceTransformer extends AttributeTransformer {
		private String newValue;
		
		public ForcedReplaceTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			newValue  = config.getString("newValue");
		}

		public ForcedReplaceTransformer( String newValue) {
			super(1);
			this.newValue  = newValue;
		}

		@Override
		public String[] tranform(String value) {
			transformed[0] = newValue;
			return transformed;
		}
	}	

	/**
	 * @author pranab
	 *
	 */
	public static class AnoynmizerTransformer extends AttributeTransformer {
		private String mask;
		
		public AnoynmizerTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			mask  = config.getString("mask");
		}

		public AnoynmizerTransformer( String mask) {
			super(1);
			this.mask  = mask;
		}

		@Override
		public String[] tranform(String value) {
			transformed[0] = StringUtils.repeat(mask, value.length());
			return transformed;
		}
	}	

	/**
	 * @author pranab
	 *
	 */
	public static class UniqueKeyGenerator extends AttributeTransformer {
		private String algorithm;
		
		public UniqueKeyGenerator(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			algorithm  = config.getString("algorithm");
		}

		public UniqueKeyGenerator( String algorithm) {
			super(1);
			this.algorithm  = algorithm;
		}

		@Override
		public String[] tranform(String value) {
			if (algorithm.equals("uuid")) {
				transformed[0] =  UUID.randomUUID().toString().replaceAll("-", "");
			} else {
				throw new IllegalArgumentException("invalid key generation algorithm");
			}
			return transformed;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class TrimTransformer extends AttributeTransformer {
		
		public TrimTransformer(ProcessorAttribute prAttr) {
			super(prAttr.getTargetFieldOrdinals().length);
		}

		public TrimTransformer( ) {
			super(1);
		}

		@Override
		public String[] tranform(String value) {
			transformed[0] =   value.trim();
			return transformed;
		}
	}	
	
	/**
	 * @author pranab
	 *
	 */
	public static class ConstantGenerator extends AttributeTransformer {
		private String constValue;
		
		public ConstantGenerator(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			constValue  = config.getString("constValue");
		}

		public ConstantGenerator( String constValue) {
			super(1);
			this.constValue  = constValue;
		}

		@Override
		public String[] tranform(String value) {
			transformed[0] = constValue;
			return transformed;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class GroupTransformer extends AttributeTransformer {
		private Map<String, List<String>> groupValues = new HashMap<String, List<String>>();
		
		public GroupTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			
			Set<Entry<String, ConfigValue>>   entries = config.entrySet();
			for (Entry<String, ConfigValue> entry : entries) {
				String[] values = entry.getValue().unwrapped().toString().split(",");
				groupValues.put(entry.getKey(), Arrays.asList(values));
			}
		}

		public GroupTransformer( Map<String, List<String>> groupValues ) {
			super(2);
			this.groupValues  = groupValues;
		}

		@Override
		public String[] tranform(String value) {
			transformed[0] = value;
			String group = null;
			for (String key : groupValues.keySet()) {
				if (groupValues.get(key).contains(value)) {
					group = key;
					break;
				}
			}
			if (null == group) {
				throw new IllegalArgumentException("no group found");
			}
			transformed[1] = group;
			return transformed;
		}
	}	
	
	/**
	 * @author pranab
	 *
	 */
	public static class StringCustomTransformer extends CustomTransformer {
		
		public StringCustomTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr, config);
		}		
		
		public StringCustomTransformer(String script, Map<String, Object> params) {
			super(script,  params);
		}

		protected  Object getFieldValue(String value) {
			return value;
		}
		
		protected  String getOutput(Object out) {
			String ret = null;
			if (out instanceof String ) {
				ret = "" + (String)out;
			} else {
				throw new IllegalArgumentException("string output expected");
			}
			return ret;
		}
	}
	
}
