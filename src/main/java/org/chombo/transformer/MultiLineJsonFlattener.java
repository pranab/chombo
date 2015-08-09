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

/**
 * Flattens multi line JSON
 * @author pranab
 *
 */
public class MultiLineJsonFlattener {
	private StringBuilder flattenedLineBld = new StringBuilder();
	private String falttenedRec;
	private int braceMatchCount;
	private String[] items;

	/**
	 * @param rawLine
	 * @return
	 */
	public String processRawLine(String rawLine) {
		falttenedRec = null;
		flattenedLineBld.append(" ").append(rawLine);
		items = rawLine.split("\\{");
		braceMatchCount += items.length -1;
		items = rawLine.split("\\}");
		braceMatchCount -= items.length -1;
		
		if (braceMatchCount == 0) {
			//got complete JSON
			falttenedRec = flattenedLineBld.toString();
			flattenedLineBld.delete(0, flattenedLineBld.length());
		}
		return falttenedRec;
	}
}
