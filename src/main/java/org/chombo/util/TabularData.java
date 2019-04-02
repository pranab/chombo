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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;

/**
 * Tabular data structure
 * @author pranab
 *
 */
public class TabularData implements Serializable {
	protected int[][] table;
	protected int numRow;
	protected int numCol;
	protected String[] rowLabels;
	protected String[] colLabels;
	protected static final String DELIMETER = ",";
	
	/**
	 * 
	 */
	public TabularData() {
	}
	
	/**
	 * @param fs
	 * @throws IOException
	 */
	public TabularData(InputStream fs) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
		String line = null; 
    	int row = 0;
    	String[] rowLabels = null;
    	String[] colLabels = null;
    	int cellCount = 0;
		while((line = reader.readLine()) != null) {
			if (row == 0) {
				rowLabels = line.split(DELIMETER);
			} else if (row == 1) {
				colLabels = line.split(DELIMETER);
				initialize(rowLabels.length,  colLabels.length);
				setLabels(rowLabels, colLabels); 
			} else {
				String[] values = line.split(DELIMETER);
				for (int col = 0; col < values.length; ++col) {
					set(row - 2, col, Integer.parseInt(values[col]));
					++cellCount;
				}
			}
			++row;
		}
		if (row - 2 != numRow) {
			BasicUtils.assertFail("incorrect number of row data");
		}
		
		if (cellCount < numRow * numCol) {
			//lower triangular defined
			if (numRow == numCol) {
				if (cellCount == numRow * (numRow -1)) {
					for (int r = 0; r < numRow; ++r) {
						for (int c = 0; c < numCol; ++c) {
							if (c > r) {
								table[r][c] = table[c][r];
							}
						}
					}
				} else {
					BasicUtils.assertFail("incorrect number of lower disagonal elements for square matrix");
				}
			} else {
				BasicUtils.assertFail("only square matrix can be partically specified");
			}
		}
	}
	
	/**
	 * @param numRow
	 * @param numCol
	 */
	public TabularData(int numRow, int numCol) {
		initialize( numRow,  numCol);
	}
	
	/**
	 * @param rowLabels
	 * @param colLabels
	 */
	public TabularData(String[] rowLabels, String[] colLabels) {
		initialize(rowLabels.length,  colLabels.length);
		setLabels(rowLabels, colLabels); 
	}

	/**
	 * @param numRow
	 * @param numCol
	 */
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
	public void set(int row, int col, int val) {
		table[row][col] = val;
	}
	
	/**
	 * sets value to cell
	 * @param rowLabel
	 * @param colLabel
	 * @param val
	 */
	public void set(String rowLabel, String colLabel, int val) {
		int[] rowCol = getRowCol(rowLabel, colLabel);
		table[rowCol[0]][rowCol[1]] = val;
	}
	
	/**
	 * @param row
	 * @param col
	 * @param val
	 */
	public void setAll(int val) {
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				table[r][c] = val;
			}
		}
	}

	/**
	 * @param row
	 * @param col
	 * @return
	 */
	public int get(int row, int col) {
		return table[row][col];
	}

	/**
	 * @param rowLabel
	 * @param colLabel
	 * @return
	 */
	public int get(String rowLabel, String colLabel) {
		int[] rowCol = getRowCol(rowLabel, colLabel);
		return table[rowCol[0]][rowCol[1]];
	}
	
	/**
	 * @param row
	 * @return
	 */
	public int[] getRow(int row) {
		return table[row];
	}
	
	/**
	 * @param row
	 * @param col
	 * @param val
	 */
	public void add(int row, int col, int val) {
		table[row][col] += val;
	}

	/**
	 * add value to cell
	 * @param rowLabel
	 * @param colLabel
	 * @param val
	 */
	public void add(String rowLabel, String colLabel, int val) {
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
	public int getRowSum(int row) {
		int sum = 0;
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
	public int getColumnSum(int col) {
		int sum = 0;
		for (int r = 0; r < numRow; ++r) {
			sum += table[r][col];
		}
		return sum;
	}
	
	/**
	 * @return
	 */
	public int getSum() {
		int sum = 0;
		for (int r = 0; r < numRow; ++r) {
			sum += getRowSum(r);
		}
		return sum;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return serialize();
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
				table[r][c]  = Integer.parseInt(items[k++]);
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
			table[row][c]  = Integer.parseInt(items[k++]);
		}
	}
	
	/**
	 * Row and column index
	 * @param rowLabel
	 * @param colLabel
	 * @return
	 */
	public int[] getRowCol(String rowLabel, String colLabel) {
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

	/**
	 * @return
	 */
	public int getNumRow() {
		return numRow;
	}

	/**
	 * @return
	 */
	public int getNumCol() {
		return numCol;
	}

}
