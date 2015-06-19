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

/**
 * Flattens multi line record to singlr line
 * @author pranab
 *
 */
public class MultiLineFlattener {
	private List<Pattern> begPatterns = new ArrayList<Pattern>();
	private List<String> begStrings;
	private boolean regExRecordBegDetectors;
	private StringBuilder flattenedLineBld = new StringBuilder();
	private String falttenedRec;
	private Matcher matcher;
	
	/**
	 * @param rawSchema
	 */
	public MultiLineFlattener(RawAttributeSchema rawSchema) {
		regExRecordBegDetectors = rawSchema.isRegExRecordBegDetectors();
		if (regExRecordBegDetectors) {
			for (String regEx : rawSchema.getRecordBegDetectors()) {
				begPatterns.add(Pattern.compile(regEx));
			}
		} else {
			begStrings = rawSchema.getRecordBegDetectors();
		}
	}
	
	/**
	 * @param rawLine
	 * @return
	 */
	public String processRawLine(String rawLine) {
		falttenedRec = null;
		boolean newRecord = false;
		
		if (regExRecordBegDetectors) {
			//pattern
			for (Pattern begPattern : begPatterns) {
				matcher = begPattern.matcher(rawLine);
				if (matcher.find()) {
					newRecord = true;
					break;
				}
			}
		} else {
			//string
			for (String begString : begStrings) {
				if (rawLine.startsWith(begString)) {
					newRecord = true;
					break;
				}
			}
		}
		
		if (newRecord) {
			if (flattenedLineBld.length() > 0) {
				falttenedRec = flattenedLineBld.toString();
				flattenedLineBld.delete(0, flattenedLineBld.length());
			}
		} else {
			flattenedLineBld.append(" ").append(rawLine);
		}
		
		return falttenedRec;
	}
	
	/**
	 * @return
	 */
	public String processCleanup() {
		falttenedRec = null;
		if (flattenedLineBld.length() > 0) {
			falttenedRec = flattenedLineBld.toString();
			flattenedLineBld.delete(0, flattenedLineBld.length());
		}
		
		return falttenedRec;
	}

}
