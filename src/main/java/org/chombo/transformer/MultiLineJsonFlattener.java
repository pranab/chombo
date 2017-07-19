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

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.chombo.util.BasicUtils;

/**
 * Flattens multi line JSON
 * @author pranab
 *
 */
public class MultiLineJsonFlattener implements Serializable {
	private StringBuilder flattenedLineBld = new StringBuilder();
	private String rec;
	private int braceMatchCount;
	private int lineCounter;
	private Pattern openBrace;
	private Pattern closeBrace;
	private boolean atBegin = true;
	private int numBraces;
	private boolean debugOn;

	/**
	 * 
	 */
	public MultiLineJsonFlattener() {
		openBrace = Pattern.compile("\\{");
		closeBrace = Pattern.compile("\\}");
	}
	
	public void setDebugOn(boolean debugOn) {
		this.debugOn = debugOn;
	}

	/**
	 * @param rawLine
	 * @return
	 */
	public String processRawLine(String rawLine) { 
		String current = null;
		rec = null;
		flattenedLineBld.append(" ").append(rawLine);
		String jsonRec = null;
		++lineCounter;
		if (debugOn)
			System.out.println("lineCounter: " + lineCounter + " raw line: " + rawLine);
		
		if (atBegin) {
			if (rawLine.indexOf("[") != -1) {
				truncateBegin();
				atBegin = false;
			} else {
				if (debugOn)
					System.out.println("still in begin segment");
			}
		} else {
			current = flattenedLineBld.toString();
			braceCount(current);
			
			//got complete JSON
			if (numBraces > 0 && braceMatchCount == 0) {
				rec = flattenedLineBld.toString();
				int firstBrace = rec.indexOf("{");
				int lastBrace = rec.lastIndexOf("}");
				System.out.println("brace matched  rec:" + rec);
				if (lastBrace == rec.length() - 1) {
					//line ends with }
					jsonRec = rec.substring(firstBrace);
					flattenedLineBld.delete(0, flattenedLineBld.length());
				} else {
					//there is more in the line after }
					++lastBrace;
					jsonRec = rec.substring(firstBrace, lastBrace);
					flattenedLineBld.delete(0, flattenedLineBld.length());
					flattenedLineBld.append(rec.substring(lastBrace));
				}
			} else {
				if (debugOn)
					System.out.println("brace not matched");
			}
		}
		
		return jsonRec;
	}
	
	/**
	 * 
	 */
	private void truncateBegin() {
		String current = null;
		//remove {..[ from beginning
		String lines = flattenedLineBld.toString();
		int pos = lines.indexOf("[");
		if (pos == -1) {
			throw new IllegalStateException("invalid json");
		} else {
			flattenedLineBld.delete(0, flattenedLineBld.length());
			current = lines.substring(pos + 1);
			flattenedLineBld.append(current);
			braceCount(current);
		}
		if (debugOn)
			System.out.println("after trucating doc begin: " + flattenedLineBld.toString());
	}
	
	/**
	 * @param current
	 */
	private void braceCount(String current) {
		int firstOpBrace = current.indexOf("{");
		int lastClBrace = current.lastIndexOf("}");
		int start = firstOpBrace != -1 ? firstOpBrace : 0;
		int end = lastClBrace != -1 ? lastClBrace + 1 : current.length();
		String sub = current.substring(start, end);
		
		braceMatchCount = numBraces = BasicUtils.findNumOccureneces(sub, openBrace);
		braceMatchCount -= BasicUtils.findNumOccureneces(sub, closeBrace);
	}
}
