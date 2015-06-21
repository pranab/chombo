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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.chombo.util.BaseAttribute;

/**
 * Extracting structured fields from, raw field
 * @author pranab
 *
 */
public class RawAttribute extends BaseAttribute {
	private int numDerivedAttributes;
	private boolean verbatim;
	private String regEx;
	private List<int[]> subSequenceIndexes;
	private Pattern pattern;
	private List<String> attrTranformers = new ArrayList<String>();
	private Matcher matcher;
	private List<String> jsonPaths = new ArrayList<String>();
	
	public static final String TRANS_ALL_CAP = "allCap";
	public static final String TRANS_ALL_SMALL = "allSmall";
	public static final String TRANS_CAMEL = "camel";
	
	
	public int getNumDerivedAttributes() {
		return numDerivedAttributes;
	}
	
	public void setNumDerivedAttributes(int numDerivedAttributes) {
		this.numDerivedAttributes = numDerivedAttributes;
	}
	
	public boolean isVerbatim() {
		return verbatim;
	}

	public void setVerbatim(boolean verbatim) {
		this.verbatim = verbatim;
	}

	public String getRegEx() {
		return regEx;
	}
	
	public void setRegEx(String regEx) {
		this.regEx = regEx;
		
		//compile
		if (null != regEx) {
			pattern = Pattern.compile(regEx);
		}
	}
	
	public List<int[]> getSubSequenceIndexes() {
		return subSequenceIndexes;
	}
	
	public void setSubSequenceIndexes(List<int[]> subSequenceIndexes) {
		this.subSequenceIndexes = subSequenceIndexes;
	}

	public List<String> getJsonPaths() {
		return jsonPaths;
	}

	public void setJsonPaths(List<String> jsonPaths) {
		this.jsonPaths = jsonPaths;
	}

	public List<String> getAttrTranformers() {
		return attrTranformers;
	}

	public void setAttrTranformers(List<String> attrTranformers) {
		this.attrTranformers = attrTranformers;
	}
	
	public Pattern getPattern() {
		return pattern;
	}

}
