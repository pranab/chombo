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

import java.util.ArrayList;
import java.util.List;

import org.chombo.util.AttributeSchema;

/**
 * Schema from processing unstructured or JSON data
 * @author pranab
 *
 */
public class RawAttributeSchema extends AttributeSchema<RawAttribute> {
	private String recordType;
	private List<String> recordBegDetectors;
	private boolean regExRecordBegDetectors;
	private List<String> jsonPaths;
	
	public static final String REC_ONE_LINE = "oneLine";
	public static final String REC_MULTI_LINE = "multiLine";
	public static final String REC_ONE_LINE_JSON = "oneLineJson";
	public static final String REC_MULTI_LINE_JSON = "multiLineJson";
	
	
	/**
	 * @return
	 */
	public String getRecordType() {
		return recordType;
	}

	/**
	 * @param recordType
	 */
	public void setRecordType(String recordType) {
		this.recordType = recordType;
	}

	/**
	 * @return
	 */
	public List<String> getRecordBegDetectors() {
		return recordBegDetectors;
	}

	/**
	 * @param recordBegDetectors
	 */
	public void setRecordBegDetectors(List<String> recordBegDetectors) {
		this.recordBegDetectors = recordBegDetectors;
	}

	/**
	 * @return
	 */
	public boolean isRegExRecordBegDetectors() {
		return regExRecordBegDetectors;
	}

	/**
	 * @param regExrecordBegDetectors
	 */
	public void setRegExRecordBegDetectors(boolean regExRecordBegDetectors) {
		this.regExRecordBegDetectors = regExRecordBegDetectors;
	}

	public List<String> getJsonPaths() {
		return jsonPaths;
	}

	public void setJsonPaths(List<String> jsonPaths) {
		this.jsonPaths = jsonPaths;
	}

	/**
	 * @return
	 */
	public int getDerivedAttributeCount() {
		int count = 0;
		
		for (RawAttribute attr : attributes) {
			count += attr.getNumDerivedAttributes();
		}
		return count;
	}
	
}
