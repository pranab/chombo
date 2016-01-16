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

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.util.HashMap;
import java.util.Map;

import org.chombo.util.ProcessorAttribute;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

/**
 * Groovy script based custom transformer
 * @author pranab
 *
 */
public abstract class CustomTransformer extends AttributeTransformer {
	private String script;
	private Map<String, Object> params = new HashMap<String, Object>();
	private Binding binding = new Binding();
	
	public CustomTransformer(ProcessorAttribute prAttr, Config config) {
		super(prAttr.getTargetFieldOrdinals().length);
		this.script = config.getString("script");
		for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
			Object value = entry.getValue().unwrapped();
			binding.setVariable(entry.getKey(), value);
		}
	}
	
	public CustomTransformer(String script, Map<String, Object> params) {
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
