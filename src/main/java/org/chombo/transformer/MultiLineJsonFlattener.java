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

import java.util.regex.Pattern;

import org.chombo.util.BasicUtils;

/**
 * Flattens multi line JSON
 * @author pranab
 *
 */
public class MultiLineJsonFlattener {
	private StringBuilder flattenedLineBld = new StringBuilder();
	private String rec;
	private int braceMatchCount;
	private String[] items;
	private int lineCounter;
	private Pattern openBrace;
	private Pattern closeBrace;
	

	public MultiLineJsonFlattener() {
		openBrace = Pattern.compile("\\{");
		closeBrace = Pattern.compile("\\}");
	}
	
	/**
	 * @param rawLine
	 * @return
	 */
	public String processRawLine(String rawLine) {
		rec = null;
		flattenedLineBld.append(" ").append(rawLine);
		String current = flattenedLineBld.toString();
		braceMatchCount = BasicUtils.findNumOccureneces(current, openBrace);
		braceMatchCount -= BasicUtils.findNumOccureneces(current, closeBrace);
				
		String jsonRec = null;
		if (++lineCounter == 3) {
			//remove {..[ from beginning
			String lines = flattenedLineBld.toString();
			int pos = lines.indexOf("[");
			if (pos == -1) {
				throw new IllegalStateException("invalid json");
			} else {
				flattenedLineBld.delete(0, flattenedLineBld.length());
				flattenedLineBld.append(lines.substring(pos + 1));
				--braceMatchCount;
			}
		}
		
		if (braceMatchCount == 0) {
			//got complete JSON
			rec = flattenedLineBld.toString();
			int firstBrace = rec.indexOf("{");
			int lastBrace = rec.lastIndexOf("}");
			if (lastBrace == rec.length() - 1) {
				jsonRec = rec.substring(firstBrace);
				flattenedLineBld.delete(0, flattenedLineBld.length());
			} else {
				++lastBrace;
				jsonRec = rec.substring(firstBrace, lastBrace);
				flattenedLineBld.delete(0, flattenedLineBld.length());
				flattenedLineBld.append(rec.substring(lastBrace));
			}
			
		}
		return jsonRec;
	}
}
