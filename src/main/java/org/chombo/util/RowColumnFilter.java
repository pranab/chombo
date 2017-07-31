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

package org.chombo.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * @author pranab
 *
 */
public class RowColumnFilter {
	private String[] rowKeys;
	private int[] colOrdinals;

	public RowColumnFilter() {
	}
	
	/**
	 * @param rowStream
	 * @param colStream
	 * @param delim
	 * @throws IOException
	 */
	public RowColumnFilter(InputStream rowStream, InputStream colStream, String delim) throws IOException {
		//row Keys
		processRows(rowStream,  delim);
		
		//column ordinals
		processColumns(colStream,  delim);
	}
	
	/**
	 * @param rowStream
	 * @param delim
	 * @throws IOException
	 */
	public void processRows(InputStream rowStream, String delim) throws IOException {
		//row Keys to be excluded
		List<String> rows = BasicUtils.getFileLines(rowStream);
		rowKeys = new String[rows.size()];
		int i = 0;
		for (String row : rows) {
			String[] items = BasicUtils.splitOnFirstOccurence(row, delim, true);
			rowKeys[i++] = items[0];
		}
	}
	
	/**
	 * @param colStream
	 * @param delim
	 * @throws IOException
	 */
	public void processColumns(InputStream colStream, String delim) throws IOException {
		//column ordinals of columns to be excluded
		List<String> cols = BasicUtils.getFileLines(colStream);
		colOrdinals = new int[cols.size()];
		int i = 0;
		for (String col : cols) {
			String[] items = BasicUtils.splitOnFirstOccurence(col, delim, true);
			colOrdinals[i++] = Integer.parseInt(items[0]);
		}
		
	}

	/**
	 * @param numCols
	 * @return
	 */
	public int[] getIncludedColOrdinals(int numCols) {
		int[] inclCols = new int[numCols - colOrdinals.length];
		int j = 0;
		Arrays.sort(colOrdinals);
		for (int i = 0; i < numCols; ++i) {
			 //exclude columns
			 if(Arrays.binarySearch(colOrdinals, i) < 0) {
				 inclCols[j++] = i; 
			 }
		}
		return inclCols;
	}
	
	/**
	 * @return
	 */
	public String[] getExcludedRowKeys() {
		return rowKeys;
	}
	
}
