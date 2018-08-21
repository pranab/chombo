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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.chombo.util.BasicUtils;
import org.chombo.util.ProcessorAttribute;

import com.typesafe.config.Config;

/**
 * @author pranab
 *
 */
public class GenericValidator {

	/**
	 * @author pranab
	 *
	 */
	public static class NotMissingValidator extends Validator {
		
		public NotMissingValidator(String tag,ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return !value.isEmpty();
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class NotMissingGroupValidator extends Validator implements RowValidator{
		private List<int[]> fieldGroups;
		
		public NotMissingGroupValidator(String tag,Map<String, Object> validatorContext) {
			super(tag);
			fieldGroups = (List<int[]>)validatorContext.get("fieldGroups");
		}

		public NotMissingGroupValidator(String tag,Config validatorConfig) {
			super(tag);
			fieldGroups = new ArrayList<int[]>();
			
			List<? extends Config> groups = validatorConfig.getConfigList("fieldGroups");
			for(Config group : groups) {
				List<Integer> colOrdinals = group.getIntList("group");
				fieldGroups.add(BasicUtils.fromListToIntArray(colOrdinals));
			}
		}
		
		@Override
		public boolean isValid(String value) {
			boolean isValid = true;
			String[] items = value.split(fieldDelim, -1);
			
			//all groups
			for (int[] group : fieldGroups) {
				boolean grIsValid = false;
				for (int ord : group) {
					//at least one non empty in a group
					if (!items[ord].isEmpty()) {
						grIsValid = true;
						break;
					}
				}
				
				//all group validity
				isValid = isValid && grIsValid;
				if (!isValid) {
					break;
				}
			}
			return isValid;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class EnsureIntValidator extends Validator {
		
		public EnsureIntValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return BasicUtils.isInt(value);
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class EnsureLongValidator extends Validator {
		
		public EnsureLongValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return BasicUtils.isLong(value);
		}
	}

	/**
	 * @author pranab
	 *
	 */
	public static class EnsureDoubleValidator extends Validator {
		
		public EnsureDoubleValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return BasicUtils.isDouble(value);
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class EnsureDateValidator extends Validator {
		private DateValidatorHelper helper;
		
		public EnsureDateValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
			helper = new DateValidatorHelper(prAttr);
		}

		@Override
		public boolean isValid(String value) {
			boolean valid = false;
			if (!value.isEmpty()) {
				if (null != helper.getDateFormatter()) {
					valid =  BasicUtils.isDate(value, helper.getDateFormatter());
				} else if (helper.isEpochTimeMs()) {
					valid = BasicUtils.isLong(value) && value.length() >= 13;
				} else {
					valid = BasicUtils.isLong(value) && value.length() >= 10;
				}
			}
			return valid;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class ValueDependencyValidator extends Validator implements RowValidator {
		private List<ValueDependency> valueDependencies = new ArrayList<ValueDependency>();
		
		public ValueDependencyValidator(String tag,Config validatorConfig) {
			super(tag);
			
			List<? extends Config> dependencies = validatorConfig.getConfigList("valueDependencies");
			for (Config dependency : dependencies) {
				ValueDependency valueDependency = new ValueDependency();
				valueDependency.sourceField = dependency.getInt("sourceField");
				valueDependency.targetField = dependency.getInt("targetField");
				valueDependency.valueMap = dependency.getConfig("valueMap");
				valueDependencies.add(valueDependency);
			}
		}	
		
		@Override
		public boolean isValid(String value) {
			boolean isValid = true;
			String[] items = value.split(fieldDelim, -1);
			
			for (ValueDependency dependency : valueDependencies) {
				String source = items[dependency.sourceField];
				String target = items[dependency.targetField];
				if (dependency.valueMap.hasPath(source)) {
					List<String> expectedTargets = dependency.valueMap.getStringList(source);
					if (expectedTargets.size() == 1 && expectedTargets.get(0).isEmpty()) {
						//target should be empty
						isValid = target.isEmpty();
					} else if (expectedTargets.size() == 1 && expectedTargets.get(0).equals("*")) {
						//target should be non empty
						isValid = !target.isEmpty();
					} else {
						//target should match one item in list
						isValid = expectedTargets.contains(target);
					}
				} 
				
				if (!isValid) {
					break;
				}
			}
			
			return isValid;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	private static class ValueDependency {
		public  int sourceField;
		public  int targetField;
		public Config valueMap;
	}
	
	/**
	 * Externally piped
	 * @author pranab
	 *
	 */
	public static class PipedValidator extends Validator implements RowValidator {
		private String script;
		private String workingDir;
		private String configParams;
		private List<String> commands = new ArrayList<String>();
		
		public PipedValidator(String tag, Config validatorConfig) {
			super(tag);
			script = validatorConfig.getString("script");
			commands.add(script);
			
			if (validatorConfig.hasPath("args")) {
				List<String> argList = validatorConfig.getStringList("args");
				commands.addAll(argList);
			}
			
			workingDir = validatorConfig.getString("workingDir");
			
			List<String> configList = validatorConfig.getStringList("config");
			configParams = BasicUtils.join(configList);
			commands.add(configParams);
		}

		@Override
		public boolean isValid(String value) {
			List<String> finalCommands = new ArrayList<String>(commands);
			finalCommands.add(value);
			String result = BasicUtils.execShellCommand(finalCommands, workingDir);
			return result.startsWith("valid");
		}
	}

	
}
