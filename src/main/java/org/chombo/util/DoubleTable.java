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

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

/**
 * @author pranab
 *
 */
public class DoubleTable implements Serializable {
	protected double[][] table;
	protected int numRow;
	protected int numCol;
	protected String[] rowLabels;
	protected String[] colLabels;
	protected static final String DELIMETER = ",";
	private int outputPrecision = 6;
	
	/**
	 * 
	 */
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
	 * @param rowLabels
	 * @param colLabels
	 */
	public DoubleTable(List<String> rowLabels, List<String> colLabels) {
		initialize( rowLabels.size(),  colLabels.size());
		String[] rowLabelsAr = rowLabels.toArray(new String[0]);
		String[] colLabelsAr = colLabels.toArray(new String[0]);
		setLabels(rowLabelsAr, colLabelsAr); 
	}

	/**
	 * @param numRow
	 * @param numCol
	 */
	public DoubleTable initialize(int numRow, int numCol) {
		return initialize(numRow, numCol, 0);
	}
	
	/**
	 * @param numRow
	 * @param numCol
	 * @param value
	 */
	public DoubleTable  initialize(int numRow, int numCol, double value) {
		table = new double[numRow][numCol];
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				table[r][c] = value;
			}
		}
		this.numRow = numRow;
		this.numCol = numCol;
		return this;
	}
	
	/**
	 * @param value
	 */
	public DoubleTable initializeDiagonal(double value) {
		if (numRow != numCol) {
			throw new IllegalStateException("table is not square");
		}
		for (int i = 0; i < numRow; ++i) {
			table[i][i] = value;
		}
		return this;
	}

	/**
	 * @return
	 */
	public int getNumRow() {
		return numRow;
	}

	/**
	 * @param numRow
	 */
	public void setNumRow(int numRow) {
		this.numRow = numRow;
	}

	/**
	 * @return
	 */
	public int getNumCol() {
		return numCol;
	}

	/**
	 * @param numCol
	 */
	public void setNumCol(int numCol) {
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
	 * @param rowLabel
	 * @param colLabel
	 * @param val
	 */
	public void set(String rowLabel, String colLabel, double val) {
		int row = ArrayUtils.indexOf(rowLabels, rowLabel);
		int col = ArrayUtils.indexOf(colLabels, colLabel);
		table[row][col] = val;
	}

	public void setOutputPrecision(int outputPrecision) {
		this.outputPrecision = outputPrecision;
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
	 * @param rowLabel
	 * @param colLabel
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
	 * @param rowLabel
	 * @return
	 */
	public double[] getRow(String rowLabel) {
		return table[getRowIndex(rowLabel)];
	}

	/**
	 * @param rowNum
	 * @param row
	 */
	public void getRow(int rowNum, double[] row) {
		for (int c = 0; c < numCol; ++c) {
			row[c] = table[rowNum][c];
		}
	}

	/**
	 * @param colNum
	 * @return
	 */
	public double[] getColumn(int colNum) {
		double[] column = new double[numRow];
		getColumn(colNum, column);
		return column;
	}

	/**
	 * @param colLabel
	 * @return
	 */
	public double[] getColumn(String colLabel) {
		return getColumn(getColIndex(colLabel));
	}
	
	/**
	 * @param rowNum
	 * @param row
	 */
	public void getColumn(int colNum, double[] column) {
		for (int r = 0; r < numRow; ++r) {
			column[r] = table[r][colNum];
		}
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
	 * @param rowLabel
	 * @return
	 */
	public double getRowSum(String rowLabel) {
		return getRowSum(getRowIndex(rowLabel));
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
	 * @param rowLabel
	 * @return
	 */
	public double getColumnSum(String colLabel) {
		return getColumnSum(getColIndex(colLabel));
	}

	/**
	 * @param row
	 * @param scale
	 */
	public void scaleRow(int row, double scale) {
		for (int c = 0; c < numCol; ++c) {
			table[row][c] *= scale;
		}
	}
	
	/**
	 * @param rowLabel
	 * @param scale
	 */
	public void scaleRow(String rowLabel, double scale) {
		scaleRow(getRowIndex(rowLabel), scale);
	}

	/**
	 * @param col
	 * @param scale
	 */
	public void scaleColumn(int col, double scale) {
		for (int r = 0; r < numRow; ++r) {
			table[r][col] *= scale;
		}
	}

	/**
	 * @param colLabel
	 * @param scale
	 */
	public void scaleColumn(String colLabel, double scale) {
		scaleColumn(getColIndex(colLabel), scale);
	}
	
	/**
	 * @return
	 */
	public double getMaxElement() {
		double max = Double.MIN_VALUE;
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				if (table[r][c] > max) {
					max = table[r][c];
				}
			}
		}
		
		return max;
	}
	
	/**
	 * @return
	 */
	public double getMaxDiagonalElement() {
		if (numRow != numCol) {
			throw new IllegalStateException("table is not square");
		}
		double max = Double.MIN_VALUE;
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				if (table[r][c] > max) {
					max = table[r][c];
				}
			}
		}
		return max;
	}

	/**
	 * @return
	 */
	public double getMinDiagonalElement() {
		if (numRow != numCol) {
			throw new IllegalStateException("table is not diagonal");
		}
		double min = Double.MAX_VALUE;
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				if (table[r][c] < min) {
					min = table[r][c];
				}
			}
		}
		return min;
	}
	
	/**
	 * @return
	 */
	public boolean isDisagonal() {
		return numRow == numCol;
	}
	
	/**
	 * @param scale
	 */
	public void scale(double scale) {
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				table[r][c] /= scale;
			}
		}
	}	 

	/**
	 * serializes table
	 * @return
	 */
	public String serialize() {
		StringBuilder stBld = new StringBuilder();
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				stBld.append(BasicUtils.formatDouble(table[r][c], outputPrecision)).append(DELIMETER);
			}
			
		}
		
		return stBld.substring(0, stBld.length()-1);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return serialize();
	}

	/**
	 * @return
	 */
	public String serializeTabular() {
		StringBuilder stBld = new StringBuilder();
		for (int r = 0; r < numRow; ++r) {
			stBld.append(serializeRow(r)).append("\n");
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
			stBld.append(BasicUtils.formatDouble(table[row][c], outputPrecision)).append(DELIMETER);
		}
		
		return stBld.substring(0, stBld.length()-1);
	}

	/**
	 * serialize row
	 * @param rowLabel
	 * @return
	 */
	public String serializeRow(String rowLabel) {
		return serializeRow(getRowIndex(rowLabel));
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
	 * @param items
	 * @param beg
	 * @param end
	 */
	public void deseralize(String[] items, int beg) {
		int k = beg;
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
	 * deserialize row
	 * @param data
	 * @param row
	 */
	public void deseralizeRow(String data, String rowLabel) {
		deseralizeRow(data, getRowIndex(rowLabel));
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
	
	/**
	 * Row index
	 * @param rowLabel
	 * @return
	 */
	private int getRowIndex(String rowLabel) {
		int row = BasicUtils.getIndex(rowLabels, rowLabel);
		return row;
	}

	/**
	 * Column index
	 * @param colLabel
	 * @return
	 */
	private int getColIndex(String colLabel) {
		int col = BasicUtils.getIndex(colLabels, colLabel);
		return col;
	}
}
