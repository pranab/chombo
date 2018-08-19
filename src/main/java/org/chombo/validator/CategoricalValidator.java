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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.chombo.util.BasicUtils;
import org.chombo.util.ProcessorAttribute;

import com.typesafe.config.Config;

/**
 * @author pranab
 *
 */
public class CategoricalValidator {
	
	/**
	 * @author pranab
	 *
	 */
	public static class MembershipValidator extends Validator {

		public MembershipValidator(String tag, ProcessorAttribute prAttr) {
			super(tag,  prAttr);
		}

		@Override
		public boolean isValid(String value) {
			return prAttr.getCardinality().contains(value);
		}
	}

	
	/**
	 * @author pranab
	 *
	 */
	public static class MembershipValidatorExtSource extends Validator {
		private String filePath;
		private int colIndex;
		private String delim;
		private List<String> cardinality = new ArrayList<String>();
		
		public MembershipValidatorExtSource(String tag, ProcessorAttribute prAttr, Config config)  {
			super(tag,  prAttr);
			
			filePath = config.getString("filePath");
			colIndex = config.getInt("colIndex");
			delim = config.getString("delim");
			List<String> lines = null;
			try {
				lines = BasicUtils. getFileLines(filePath);
			} catch (IOException e) {
				throw new IllegalStateException("falied to load member values from file");
			}
			for (String line : lines) {
				String[] items = line.split(delim, -1);
				cardinality.add(items[colIndex]);
			}
		}

		@Override
		public boolean isValid(String value) {
			return cardinality.contains(value);
		}
	}
	
}
