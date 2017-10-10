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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.chombo.util.AttributePredicate;
import org.chombo.util.BasicUtils;
import org.chombo.util.Pair;
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

		/**
		 * @param prAttr
		 */
		public LowerCaseTransformer(ProcessorAttribute prAttr) {
			super(prAttr.getTargetFieldOrdinals().length);
		}

		/**
		 * 
		 */
		public LowerCaseTransformer() {
			super(1);
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
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
		
		/**
		 * @param prAttr
		 */
		public UpperCaseTransformer(ProcessorAttribute prAttr) {
			super(prAttr.getTargetFieldOrdinals().length);
		}
		
		/**
		 * 
		 */
		public UpperCaseTransformer() {
			super(1);
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
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
		private boolean retainOriginalField;
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public PatternBasedTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			pattern = Pattern.compile(config.getString("regEx"));
			failOnMissingGroup = config.getBoolean("failOnMissingGroup");
			retainOriginalField = config.getBoolean("retainOriginalField");
		}

		/**
		 * @param numTransAttributes
		 * @param regEx
		 * @param failOnMissingGroup
		 */
		public PatternBasedTransformer(int numTransAttributes, String regEx, boolean failOnMissingGroup) {
			super(numTransAttributes);
			pattern = Pattern.compile(regEx);
			this.failOnMissingGroup =  failOnMissingGroup;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
		@Override
		public String[] tranform(String value) {
			matcher = pattern.matcher(value);
			if (matcher.matches()) {
				int grIndx = 1;
				for (int i = 0; i < transformed.length; ++i) {
					if (retainOriginalField && i == 0) {
						transformed[i] = value;
						continue;
					}
					
					//create new fields by extracting group
			        String extracted = matcher.group(grIndx);
			        if(extracted != null) {
			        	transformed[i] = extracted;
			        } else {
			        	if (failOnMissingGroup) {
			        		throw new IllegalArgumentException("mtaching failed for a group in  pattern based transformer");
			        	} else {
			        		transformed[i] = "";
			        	}
			        }
			        ++grIndx;
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
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public SearchReplaceTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			regEx  = config.getString("regEx");
			replacement = config.getString("replacement");
			replaceAll = config.getBoolean("replaceAll");
		}
		
		/**
		 * @param numTransAttributes
		 * @param regEx
		 * @param replacement
		 * @param replaceAll
		 */
		public SearchReplaceTransformer(int numTransAttributes, String regEx, String replacement, boolean replaceAll) {
			super(numTransAttributes);
			this.regEx  = regEx;
			this.replacement = replacement;
			this.replaceAll = replaceAll;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
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
	public static class PatternBasedSearchReplaceTransformer extends AttributeTransformer {
		private String replacement;
		private boolean replacementLengthSame;
		private Matcher matcher;
		private Set<String> searchResults = new HashSet<String>();
		private List<Pair<Pattern, Integer>> patterns = new ArrayList<Pair<Pattern, Integer>>();
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public PatternBasedSearchReplaceTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			replacement = config.getString("replacement");
			replacementLengthSame = config.getBoolean("replacementLengthSame");
			
			//all patterns
			List<? extends Config> patternConfigs = config.getConfigList("patterns");
			for (Config patConfig : patternConfigs) {
				String regEx = patConfig.getString("regEx");
				int numGroups = patConfig.getInt("numGroups");
				patterns.add(new Pair<Pattern, Integer>(Pattern.compile(regEx), numGroups));
			}
		}
		
		/**
		 * @param numTransAttributes
		 * @param regExes
		 * @param replacement
		 * @param numGroups
		 * @param replacementLengthSame
		 */
		public PatternBasedSearchReplaceTransformer(int numTransAttributes, String[] regExes, String replacement, 
				int[] numGroups, boolean replacementLengthSame) {
			super(numTransAttributes);
			this.replacement = replacement;
			this.replacementLengthSame = replacementLengthSame;
			for (int i = 0; i < regExes.length; ++i) {
				patterns.add(new Pair<Pattern, Integer>(Pattern.compile(regExes[i]), numGroups[i]));
			}
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
		@Override
		public String[] tranform(String value) {
			searchResults.clear();
			
			//all patterns
			boolean matched = false;
			for (Pair<Pattern, Integer> pattern : patterns) {
				matcher = pattern.getLeft().matcher(value);
				int numGroups = pattern.getRight();
				if (-1 == numGroups) {
					//variable number of groups
			    	while(matcher.find()) {
			    		String found = matcher.group(1);
			    		searchResults.add(found);
			    		matched = true;
			    	}
				} else {
					//known number of groups
					if (matcher.matches()) {
			        	for (int gr = 1; gr <= numGroups; ++gr) {
			        		String found = matcher.group(gr);
			        		//System.out.println("found:" + found)
				    		searchResults.add(found);
			        	}
			        	matched = true;
			    	}				
				}
		    	if (matched) {
		    		break;
		    	}
			}
			
			//all replacements
			String replaced = value;
			for (String found : searchResults) {
				String replacement = getReplacement(found);
				replaced = replaced.replace(found, replacement);
			}
			transformed[0] = replaced;
			return transformed;
		}
		
		/**
		 * @param found
		 * @return
		 */
		private String getReplacement(String found) {
			String replacement = null;
			if (replacementLengthSame) {
				//create mask same size as string being replaced
				if (this.replacement.length() != 1) {
					throw new IllegalStateException("mask size should be 1");
				}
				replacement = StringUtils.repeat(this.replacement, found.length());
			} else {
				//use as is
				replacement = this.replacement;
			}
			return replacement;
		}
		
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class KeyValueTransformer extends AttributeTransformer {
		private Config keyValConfig;
		private Map<String, String>  kayValues;
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public KeyValueTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			if (config.hasPath("keyValues")) {
				keyValConfig = config.getConfig("keyValues");
			} 
		}

		/**
		 * @param kayValues
		 */
		public KeyValueTransformer(Map<String, String>  kayValues) {
			super(1);
			this.kayValues = kayValues;
		}

		/**
		 * @param prAttr
		 * @param config
		 * @param inStrm
		 * @throws IOException
		 */
		public KeyValueTransformer(ProcessorAttribute prAttr, Config config, InputStream inStrm) throws IOException {
			super(1);
			int fieldOrd = prAttr.getOrdinal();
			String delim = config.getString("fieldDelim");
			try {
				kayValues = new HashMap<String, String>();
	    		BufferedReader reader = new BufferedReader(new InputStreamReader(inStrm));
	    		String line = null; 
	    		while((line = reader.readLine()) != null) {
	    			String[] items = line.split(delim);
	    			if (Integer.parseInt(items[0]) == fieldOrd) {
	    				kayValues.put(items[1], items[2]);
	    			}
	    		}
			} catch (IOException ex) {
				throw ex;
			} finally {
				inStrm.close();
			}
		}
		
		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
		@Override
		public String[] tranform(String value) {
			String newValue = null;
			if (null != keyValConfig) {
				if (keyValConfig.hasPath(value)) {
					newValue = keyValConfig.getString(value);
				} else {
					newValue = null;
				}
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
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public DefaultValueTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			defaultValue  = config.getString("defaultValue");
		}

		/**
		 * @param defaultValue
		 */
		public DefaultValueTransformer( String defaultValue) {
			super(1);
			this.defaultValue  = defaultValue;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
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
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public ForcedReplaceTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			newValue  = config.getString("newValue");
		}

		/**
		 * @param newValue
		 */
		public ForcedReplaceTransformer( String newValue) {
			super(1);
			this.newValue  = newValue;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
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
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public AnoynmizerTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			mask  = config.getString("mask");
		}

		/**
		 * @param mask
		 */
		public AnoynmizerTransformer( String mask) {
			super(1);
			this.mask  = mask;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
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
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public UniqueKeyGenerator(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			algorithm  = config.getString("algorithm");
		}

		/**
		 * @param algorithm
		 */
		public UniqueKeyGenerator( String algorithm) {
			super(1);
			this.algorithm  = algorithm;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
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
		
		/**
		 * @param prAttr
		 */
		public TrimTransformer(ProcessorAttribute prAttr) {
			super(prAttr.getTargetFieldOrdinals().length);
		}

		/**
		 * 
		 */
		public TrimTransformer( ) {
			super(1);
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
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
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public ConstantGenerator(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			constValue  = config.getString("constValue");
		}

		/**
		 * @param constValue
		 */
		public ConstantGenerator( String constValue) {
			super(1);
			this.constValue  = constValue;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
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
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public GroupTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			
			Set<Entry<String, ConfigValue>>   entries = config.entrySet();
			for (Entry<String, ConfigValue> entry : entries) {
				String[] values = entry.getValue().unwrapped().toString().split(",");
				groupValues.put(entry.getKey(), Arrays.asList(values));
			}
		}

		/**
		 * @param groupValues
		 */
		public GroupTransformer( Map<String, List<String>> groupValues ) {
			super(2);
			this.groupValues  = groupValues;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
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
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public StringCustomTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr, config);
		}		
		
		/**
		 * @param script
		 * @param params
		 */
		public StringCustomTransformer(String script, Map<String, Object> params) {
			super(script,  params);
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.CustomTransformer#getFieldValue(java.lang.String)
		 */
		protected  Object getFieldValue(String value) {
			return value;
		}
		
		/* (non-Javadoc)
		 * @see org.chombo.transformer.CustomTransformer#getOutput(java.lang.Object)
		 */
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
	
	/**
	 * @author pranab
	 * Removes a field by returning null for transformed value
	 */
	public static class DeleteTransformer extends AttributeTransformer {
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public DeleteTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
		}


		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
		@Override
		public String[] tranform(String value) {
			return null;
		}
	}	
	
	/**
	 * Does string append or prepend with provided string
	 * @author pranab
	 *
	 */
	public static class ConcatenatorTransformer extends AttributeTransformer {
		private String operation;
		private String stringToAdd;
		private String delimiter;
		
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public ConcatenatorTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			operation  = config.getString("operation");
			stringToAdd = config.getString("stringToAdd");
			delimiter = config.getString("delimiter");
		}
		
		/**
		 * @param numTransAttributes
		 * @param operation
		 * @param stringToAdd
		 * @param delimiter
		 */
		public ConcatenatorTransformer(int numTransAttributes, String operation, String stringToAdd, String delimiter) {
			super(numTransAttributes);
			this.operation  = operation;
			this.stringToAdd = stringToAdd;
			this.delimiter = delimiter;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
		@Override
		public String[] tranform(String value) {
			if (operation.equals("prepend")) {
				transformed[0] = stringToAdd + delimiter + value;
			} else if (operation.equals("append")){
				transformed[0] = value + delimiter + stringToAdd;
			} else {
				throw new IllegalArgumentException("invalid string concatenation operator");
			}
			return transformed;
		}
		
	}
	
	/**
	 * Merges multiple fields into one
	 * @author pranab
	 *
	 */
	public static class FieldMergeTransformer extends AttributeTransformer implements ContextAwareTransformer {
		private List<Integer> mergeFieldOrdinals;
		private String delimiter;
		private String[] fields;
		
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public FieldMergeTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			config = getFieldSpecificConfig(prAttr.getOrdinal(), config);
			mergeFieldOrdinals  = config.getIntList("mergeFieldOrdinals");
			delimiter = config.getString("delimiter");
		}
		
		/**
		 * @param numTransAttributes
		 * @param mergeFieldOrdinals
		 * @param delimiter
		 */
		public FieldMergeTransformer(int numTransAttributes, List<Integer> mergeFieldOrdinals, String delimiter) {
			super(numTransAttributes);
			this.mergeFieldOrdinals  = mergeFieldOrdinals;
			this.delimiter = delimiter;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
		@Override
		public String[] tranform(String value) {
			StringBuilder stBld = new StringBuilder(value);
			for (int otherOrd : mergeFieldOrdinals) {
				stBld.append(delimiter).append(fields[otherOrd]);
			}
			transformed[0] = stBld.toString();
			return transformed;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.ContextAwareTransformer#setContext(java.util.Map)
		 */
		@Override
		public void setContext(Map<String, Object> context) {
			fields = (String[])context.get("record");
		}
		
	}
	
	/**
	 * Collapses multiple fields into one to solve filed delimiter embedded in field problem.
	 * @author pranab
	 *
	 */
	public static class WithinFieldDelimiterTransformer extends AttributeTransformer implements ContextAwareTransformer {
		private int numFieldsToCollapse = -1;
		private String replacementDelimiter = " ";
		private String[] fields;
		private int expectedNumFields;
		private int curFieldOrdinal;
		private String outputDelimiter = ",";
		private static int lastCollapsedFieldsDefined;
		private static int lastCollapsedFieldsNotDefined;
		private static boolean checkedForValidity;
		private static int collapsedFieldsNotDefinedCount;
		private static int totalNumFieldsToCollapse;
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public WithinFieldDelimiterTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			curFieldOrdinal = prAttr.getOrdinal();
			config = getFieldSpecificConfig(prAttr.getOrdinal(), config);
			
			//if not specified, then there could be only one field in the record with 
			//embedded delimiter problem
			if (config.hasPath("numFieldsToCollapse")) {
				numFieldsToCollapse = config.getInt("numFieldsToCollapse");
				if (curFieldOrdinal > lastCollapsedFieldsDefined) {
					lastCollapsedFieldsDefined = curFieldOrdinal;
				}
				++totalNumFieldsToCollapse;
			} else {
				++collapsedFieldsNotDefinedCount;
				if (curFieldOrdinal > lastCollapsedFieldsNotDefined) {
					lastCollapsedFieldsNotDefined = curFieldOrdinal;
				}
			}
			
			
			if (config.hasPath("replacementDelimiter")) {
				replacementDelimiter = config.getString("replacementDelimiter");
				if (curFieldOrdinal > lastCollapsedFieldsDefined) {
					lastCollapsedFieldsDefined = curFieldOrdinal;
				}
			} 
			
			if (config.hasPath("outputDelimiter")) {
				outputDelimiter = config.getString("outputDelimiter");
			}
			expectedNumFields = config.getInt("expectedNumFields");
		}

		/**
		 * @param numTransAttributes
		 * @param curFieldOrdinal
		 * @param numFieldsToCollapse
		 * @param replacementDelimiter
		 * @param expectedNumFields
		 * @param outputDelimiter
		 */
		public WithinFieldDelimiterTransformer(int numTransAttributes, int curFieldOrdinal, int numFieldsToCollapse,
				String replacementDelimiter, int expectedNumFields, String outputDelimiter) {
			super(numTransAttributes);
			this.curFieldOrdinal  = curFieldOrdinal;
			this.numFieldsToCollapse = numFieldsToCollapse;
			this.replacementDelimiter = replacementDelimiter;
			this.expectedNumFields = expectedNumFields;
			this.outputDelimiter = outputDelimiter;
		}
		
		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
		@Override
		public String[] tranform(String value) {
			if (expectedNumFields != fields.length) {
				checkForValidity();
				
				//get number of fields to collapse from the total field count if not specified
				int actualNumFieldsToCollapse = numFieldsToCollapse < 0 ? 
					fields.length - expectedNumFields - totalNumFieldsToCollapse: numFieldsToCollapse;
				int afterLastCollapsedFieldOrdinal = curFieldOrdinal + actualNumFieldsToCollapse + 1;
				String collapsedFields = BasicUtils.join(fields, curFieldOrdinal, afterLastCollapsedFieldOrdinal, 
					replacementDelimiter);
				
				if (numFieldsToCollapse < 0) {
					//not specified implying num of embedded delimiters could vary across rows so collapse remaining
					collapsedFields = collapsedFields + outputDelimiter + BasicUtils.join(fields, 
						afterLastCollapsedFieldOrdinal, fields.length, outputDelimiter);
				}
				transformed[0] = collapsedFields;
			} else {
				//record does not embedded delimiter issue
				if (numFieldsToCollapse < 0) {
					//collapse remaining fields
					String collapsedFields = value + outputDelimiter + BasicUtils.join(fields, 
						curFieldOrdinal + 1, fields.length, outputDelimiter);
					transformed[0] = collapsedFields;
				} else {
					transformed[0] = value;
				}
			}
			return transformed;
		}
		
		/**
		 * 
		 */
		private static void checkForValidity() {
			if (!checkedForValidity) {
				if (collapsedFieldsNotDefinedCount > 1) {
					//multiple fields with undefined number of fields to collapse not allowed
					throw new IllegalStateException(
							"mulitiple fields found where number of fields to copplapse not specified");
				} else if (collapsedFieldsNotDefinedCount == 1) {
					//multiple fields with undefined number of fields to collapse not allowed
					if (lastCollapsedFieldsNotDefined < lastCollapsedFieldsDefined) {
						throw new IllegalStateException(
								"if there is any field with undefined number of fields to collapse it should be last one");
					}
				}
				checkedForValidity = true;
			}
		}
		
		/* (non-Javadoc)
		 * @see org.chombo.transformer.ContextAwareTransformer#setContext(java.util.Map)
		 */
		@Override
		public void setContext(Map<String, Object> context) {
			fields = (String[])context.get("record");
		}
	}
	
	/**
	 * Splits string different ways
	 * @author pranab
	 *
	 */
	public static class SplitterTransformer extends AttributeTransformer {
		private String operation;
		private String delimiter;
		private boolean failOnDelimNotFound;
		private String retainPolicy;
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public SplitterTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			intialize(config.getString("operation"), config.getString("delimiter"), 
					config.getBoolean("failOnDelimNotFound"),config.getString("retainPolicy"));
		}
		
		/**
		 * @param numTransAttributes
		 * @param operation
		 * @param delimiter
		 * @param failOnDelimNotFound
		 * @param retainPolicy
		 */
		public SplitterTransformer(int numTransAttributes, String operation, String delimiter, boolean failOnDelimNotFound,
				String retainPolicy) {
			super(numTransAttributes);
			intialize(operation, delimiter, failOnDelimNotFound,retainPolicy);
		}
		
		/**
		 * @param operation
		 * @param delimiter
		 * @param failOnDelimNotFound
		 * @param retainPolicy
		 */
		public void intialize(String operation, String delimiter, boolean failOnDelimNotFound,
				String retainPolicy) {
			this.operation  = operation;
			this.delimiter = delimiter;
			this.failOnDelimNotFound = failOnDelimNotFound;
			this.retainPolicy = retainPolicy;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
		@Override
		public String[] tranform(String value) {
			String[] items = null;
			if (operation.equals("spltOnFirst")) {
				items = BasicUtils.splitOnFirstOccurence(value, delimiter, failOnDelimNotFound);
				retainOutputFields(items);
			} else if (operation.equals("spltOnLast")){
				items = BasicUtils.splitOnLastOccurence(value, delimiter, failOnDelimNotFound);
				retainOutputFields(items);
			} else if (operation.equals("spltOnAll")){
				items = value.split(delimiter, -1);
				if (items.length == transformed.length) {
					transformed = items;
				} else {
					throw new IllegalArgumentException("did not get expected number of items after splitting");
				}
			}  else {
				throw new IllegalArgumentException("invalid string splitting operator");
			}
			return transformed;
		}
		
		/**
		 * @param items
		 */
		private void retainOutputFields(String[] items) {
			if (retainPolicy.equals("first")) {
				transformed[0] = items[0];
			} else if (retainPolicy.equals("second")) {
				transformed[0] = items[1];
			} else if (retainPolicy.equals("both")) {
				transformed[0] = items[0];
				transformed[1] = items[1];
			}
		}
		
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class BinaryValueTransformer extends AttributeTransformer {
		private AttributePredicate predicate;
		private String trueValue;
		private String falseValue;
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public BinaryValueTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			initialize(config.getString("predicateExpr"), config.getString("trueValue"), 
					config.getString("falseValue"));
		}

		/**
		 * @param predicateExpr
		 * @param trueValue
		 * @param falseValue
		 */
		public BinaryValueTransformer(String predicateExpr, String trueValue, String falseValue) {
			super(1);
			initialize(predicateExpr, trueValue, falseValue);
		}

		/**
		 * @param predicateExpr
		 * @param trueValue
		 * @param falseValue
		 */
		public void initialize(String predicateExpr, String trueValue, String falseValue) {
			this.predicate = AttributePredicate.create(predicateExpr);
			this.trueValue = trueValue;
			this.falseValue = falseValue;
		}
		
		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
		@Override
		public String[] tranform(String value) {
			transformed[0] = predicate.evaluate(value) ? trueValue : falseValue;
			return transformed;
		}
	}	
	
	/**
	 * Replaces categorical value with with set of dummy binary values
	 * @author pranab
	 *
	 */
	public static class CategoricalToBinaryTransformer extends AttributeTransformer {
		private List<String> cardinality;
		
		/**
		 * @param prAttr
		 * @param config
		 */
		public CategoricalToBinaryTransformer(ProcessorAttribute prAttr, Config config) {
			super(prAttr.getTargetFieldOrdinals().length);
			cardinality  = prAttr.getCardinality();
		}

		/**
		 * @param cardinality
		 */
		public CategoricalToBinaryTransformer(List<String> cardinality) {
			super(cardinality.size());
			this.cardinality  = cardinality;
		}

		/* (non-Javadoc)
		 * @see org.chombo.transformer.AttributeTransformer#tranform(java.lang.String)
		 */
		@Override
		public String[] tranform(String value) {
			if (cardinality.contains(value)) {
				for (int i = 0; i < cardinality.size(); ++i) {
					transformed[i] = value.equals(cardinality.get(i)) ?  "1" : "0";
				}
			} else {
				throw new IllegalStateException("illegal categorical attribute value");
			}
			return transformed;
		}
	}	
	
}
