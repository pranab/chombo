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

		@Override
		public String[] tranform(String value) {
			transformed[0] =  value.toUpperCase();
			return transformed;
		}
		
	}
}
