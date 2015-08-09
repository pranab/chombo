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

import java.util.regex.Matcher;

/**
 * Extracts one or more fields from unstructured field
 * @author pranab
 *
 */
public class UnstructuredFieldExtractor {
	private RawAttributeSchema rawSchema;
	private boolean failOnInvalid;
	private Matcher matcher;
	private String[] extractedAttrs;


	/**
	 * @param rawSchema
	 * @param failOnInvalid
	 */
	public UnstructuredFieldExtractor(RawAttributeSchema rawSchema, boolean failOnInvalid) {
		this.rawSchema = rawSchema;
		this.failOnInvalid = failOnInvalid;
	}
	
	/**
	 * @param rawAttrIndex
	 * @param rawAttr
	 * @param derivedAttr
	 * @param offset
	 * @return
	 */
	public int extractAttributes(int rawAttrIndex, String rawAttr, String[]derivedAttr, int offset) {
		RawAttribute rawAttrMeta = rawSchema.findAttribute(rawAttrIndex);
		extractAttributes(rawAttr, rawAttrMeta);
		if (null != extractedAttrs) {
			for (int i = 0; i < extractedAttrs.length; ++i) {
				derivedAttr[offset+i] = extractedAttrs[i];
			}
		}
		return null != extractedAttrs ? extractedAttrs.length : 0;
	}
	
	
	/**
	 * @param rawAttr
	 * @return
	 */
	private String[] extractAttributes(String rawAttr, RawAttribute rawAttrMeta) {
		extractedAttrs = new String[rawAttrMeta.getNumDerivedAttributes()];		
		if (rawAttrMeta.isVerbatim()) {
			extractedAttrs[0] = rawAttr;
		} else if (null != rawAttrMeta.getRegEx()) {
			matcher = rawAttrMeta.getPattern().matcher(rawAttr);
			if (matcher.matches()) {
				for (int i = 0; i < rawAttrMeta.getNumDerivedAttributes(); ++i) {
			        String extracted = matcher.group(i+1);
			        if(extracted != null) {
			        	extractedAttrs[i] = extracted;
			        } else {
			        	handleInvalidData();
			        	break;
			        }
			    }
			} else {
	        	handleInvalidData();
			}			
		} else if (null != rawAttrMeta.getSubSequenceIndexes()) {
			if (rawAttrMeta.getSubSequenceIndexes().size() != rawAttrMeta.getNumDerivedAttributes()) {
				throw new IllegalStateException("number subsequence indexes is not equal to number of extracted attributes");
			}
			int i = 0;
			for (int[] indexes : rawAttrMeta.getSubSequenceIndexes()) {
				if (indexes[0] <  rawAttr.length() -1 && indexes[1] <=  rawAttr.length()) {
					String extracted = rawAttr.substring(indexes[0], indexes[1]);
					extractedAttrs[i] = extracted;
				} else {
		        	handleInvalidData();
		        	break;
				}
				++i;
			}
		} else {
			throw new IllegalStateException("valid extraction strategy must be provided");
		}
		
		return extractedAttrs;
	}
	
	/**
	 * 
	 */
	private void handleInvalidData() {
		extractedAttrs = null;
		if (failOnInvalid) {
			throw new IllegalArgumentException("failed to extract from unstructured data");
		}
	}
	
}
