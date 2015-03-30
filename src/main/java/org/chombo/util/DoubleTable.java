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

import org.apache.commons.lang3.ArrayUtils;

public class DoubleTable {
	protected double[][] table;
	protected int numRow;
	protected int numCol;
	protected String[] rowLabels;
	protected String[] colLabels;
	protected static final String DELIMETER = ",";
	
	public DoubleTable() {
	}
	
	/**
	 * @param numRow
	 * @param numCol
	 */
	public DoubleTable(int numRow, int numCol) {
		initialize( numRow,  numCol);
	}
	
	/**
	 * @param rowLabels
	 * @param colLabels
	 */
	public DoubleTable(String[] rowLabels, String[] colLabels) {
		initialize( rowLabels.length,  colLabels.length);
		setLabels(rowLabels, colLabels); 
	}

	/**
	 * @param numRow
	 * @param numCol
	 */
	public void  initialize(int numRow, int numCol) {
		table = new double[numRow][numCol];
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				table[r][c] = 0;
			}
		}
		this.numRow = numRow;
		this.numCol = numCol;
	}
	
	/**
	 * @param rowLabels
	 * @param colLabels
	 */
	public void setLabels(String[] rowLabels, String[] colLabels) {
		this.rowLabels = rowLabels;
		this.colLabels = colLabels;
	}

	/**
	 * @param row
	 * @param col
	 * @param val
	 */
	public void set(int row, int col, double val) {
		table[row][col] = val;
	}
	
	/**
	 * @param row
	 * @param col
	 * @return
	 */
	public double get(int row, int col) {
		return table[row][col];
	}

	/**
	 * @param row
	 * @param col
	 * @return
	 */
	public double get(String rowLabel, String colLabel) {
		int row = ArrayUtils.indexOf(rowLabels, rowLabel);
		int col = ArrayUtils.indexOf(colLabels, colLabel);
		return table[row][col];
	}

	/**
	 * @param row
	 * @return
	 */
	public double[] getRow(int row) {
		return table[row];
	}
	
	/**
	 * @param row
	 * @param col
	 * @param val
	 */
	public void add(int row, int col, double val) {
		table[row][col] += val;
	}

	/**
	 * add value to cell
	 * @param rowLabel
	 * @param colLabel
	 * @param val
	 */
	public void add(String rowLabel, String colLabel, double val) {
		int[] rowCol = getRowCol(rowLabel, colLabel);
		table[rowCol[0]][rowCol[1]] += val;
	}
	
	/**
	 * increments cell
	 * @param row
	 * @param col
	 */
	public void increment(int row, int col) {
		table[row][col] += 1;
	}

	/**
	 * increments cell
	 * @param rowLabel
	 * @param colLabel
	 */
	public void increment(String rowLabel, String colLabel) {
		int[] rowCol = getRowCol(rowLabel, colLabel);
		table[rowCol[0]][rowCol[1]] += 1;
	}
	
	/**
	 * sum of row
	 * @param row
	 * @return
	 */
	public double getRowSum(int row) {
		double sum = 0;
		for (int c = 0; c < numCol; ++c) {
			sum += table[row][c];
		}
		return sum;
	}

	/**
	 * sum of column
	 * @param col
	 * @return
	 */
	public double getColumnSum(int col) {
		double sum = 0;
		for (int r = 0; r < numRow; ++r) {
			sum += table[r][col];
		}
		return sum;
	}
	
	/**
	 * serializes table
	 * @return
	 */
	public String serialize() {
		StringBuilder stBld = new StringBuilder();
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				stBld.append(table[r][c]).append(DELIMETER);
			}
		}
		
		return stBld.substring(0, stBld.length()-1);
	}

	/**
	 * serialize row
	 * @param row
	 * @return
	 */
	public String serializeRow(int row) {
		StringBuilder stBld = new StringBuilder();
		for (int c = 0; c < numCol; ++c) {
			stBld.append(table[row][c]).append(DELIMETER);
		}
		
		return stBld.substring(0, stBld.length()-1);
	}

	/**
	 * deserialize table
	 * @param data
	 */
	public void deseralize(String data) {
		String[] items = data.split(DELIMETER);
		int k = 0;
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				table[r][c]  = Double.parseDouble(items[k++]);
			}
		}
	}
	
	/**
	 * deserialize row
	 * @param data
	 * @param row
	 */
	public void deseralizeRow(String data, int row) {
		String[] items = data.split(DELIMETER);
		int k = 0;
		for (int c = 0; c < numCol; ++c) {
			table[row][c]  = Double.parseDouble(items[k++]);
		}
	}
	
	/**
	 * Row and column index
	 * @param rowLabel
	 * @param colLabel
	 * @return
	 */
	private int[] getRowCol(String rowLabel, String colLabel) {
		int[] rowCol = new int[2];
		rowCol[0] = rowCol[1] = -1;

		int i = 0;
		for (String label : rowLabels) {
			if (label.equals(rowLabel)) {
				rowCol[0] = i;
				break;
			}
			++ i;
		}
		
		i = 0;
		for (String label : colLabels) {
			if (label.equals(colLabel)) {
				rowCol[1] = i;
				break;
			}
			++ i;
		}

		return rowCol;
	}

}
