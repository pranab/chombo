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

public class TabularData {
	protected int[][] table;
	protected int numRow;
	protected int numCol;
	protected String[] rowLabels;
	protected String[] colLabels;
	protected static final String DELIMETER = ",";
	
	public TabularData() {
	}
	
	public TabularData(int numRow, int numCol) {
		initialize( numRow,  numCol);
	}
	
	public TabularData(String[] rowLabels, String[] colLabels) {
		initialize( rowLabels.length,  colLabels.length);
		setLabels(rowLabels, colLabels); 
	}

	public void  initialize(int numRow, int numCol) {
		table = new int[numRow][numCol];
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				table[r][c] = 0;
			}
		}
		this.numRow = numRow;
		this.numCol = numCol;
	}
	
	public void setLabels(String[] rowLabels, String[] colLabels) {
		this.rowLabels = rowLabels;
		this.colLabels = colLabels;
	}

	public void set(int row, int col, int val) {
		table[row][col] = val;
	}
	
	public int get(int row, int col) {
		return table[row][col];
	}

	public void increment(int row, int col) {
		table[row][col] += 1;
	}

	public void increment(String rowLabel, String colLabel) {
		int row = -1;
		int col = -1;

		int i = 0;
		for (String label : rowLabels) {
			if (label.equals(rowLabel)) {
				row = i;
				break;
			}
			++ i;
		}
		
		i = 0;
		for (String label : colLabels) {
			if (label.equals(colLabel)) {
				col = i;
				break;
			}
			++ i;
		}
		
		table[row][col] += 1;
	}
	
	public String serialize() {
		StringBuilder stBld = new StringBuilder();
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				stBld.append(table[r][c]).append(DELIMETER);
			}
		}
		
		return stBld.substring(0, stBld.length()-1);
	}

	public String serializeRow(int row) {
		StringBuilder stBld = new StringBuilder();
		for (int c = 0; c < numCol; ++c) {
			stBld.append(table[row][c]).append(DELIMETER);
		}
		
		return stBld.substring(0, stBld.length()-1);
	}

	public void deseralize(String data) {
		String[] items = data.split(DELIMETER);
		int k = 0;
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				table[r][c]  = Integer.parseInt(items[k++]);
			}
		}
	}
	
	public void deseralizeRow(String data, int row) {
		String[] items = data.split(DELIMETER);
		int k = 0;
		for (int c = 0; c < numCol; ++c) {
			table[row][c]  = Integer.parseInt(items[k++]);
		}
	}

}
