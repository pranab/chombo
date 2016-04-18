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

import com.typesafe.config.Config;


/**
 * @author pranab
 *
 */
public abstract class  AttributeTransformer {
	protected String[] transformed;
	
	public AttributeTransformer() {
	}
	
	/**
	 * @param numTrans
	 */
	public AttributeTransformer(int numTrans) {
		transformed = new String[numTrans];
	}

	/**
	 * @param value
	 * @return
	 */
	public abstract String[] tranform(String value);
	
	/**
	 * Return field specific configuration if it exists, in case the same transformer is used for
	 * multiple fields
	 * @param fieldOrd
	 * @param config
	 * @return
	 */
	public Config getFieldSpecificConfig(int fieldOrd, Config config) {
		Config fieldPsecificConfig = null;
		String fieldKey = "field" + fieldOrd;
		if (config.hasPath(fieldKey)) {
			fieldPsecificConfig = config.getConfig(fieldKey);
		}
		return fieldPsecificConfig == null ? config : fieldPsecificConfig;
	}
	
}
