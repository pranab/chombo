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

/**
 * @author pranab
 *
 */
public class Matrix extends DoubleTable{
	
	/**
	 * 
	 */
	public Matrix() {
		super();
	}

	/**
	 * @param numRow
	 * @param numCol
	 */
	public Matrix(int numRow, int numCol) {
		super(numRow, numCol);
	}
	
	/**
	 * @return
	 */
	public Matrix tranpose() {
		Matrix transposed = new Matrix(numCol, numRow);
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				transposed.table[c][r] = table[r][c];
			}
		}
		return transposed;
	}
	
	/**
	 * @param that
	 * @return
	 */
	public Matrix dot(Matrix that) {
		if (this.numCol != that.numRow) {
			throw new IllegalStateException("matrix size invalid for dot product");
		}
		
		Matrix product = new Matrix(this.numRow, that.numCol);
		double[] thatColumn = new double[that.numRow];
		for (int c = 0; c < that.numCol; ++c) {
			that.getColumn(c, thatColumn);
			for (int r = 0; r < this.numRow; ++r) {
				double[] thisRow = this.getRow(r);
				double val = BasicUtils.dotProduct(thisRow, thatColumn);
				product.table[r][c] = val;
			}
		}
		
		return product;
	}
	
	/**
	 * @param that
	 * @return
	 */
	public Matrix add(Matrix that) {
		if (this.numRow != that.numRow || this.numCol != that.numCol) {
			throw new IllegalStateException("matrices should be of same size for sum");
		}
		
		Matrix sum = new Matrix(this.numRow, this.numCol);
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				sum.table[r][c] = this.table[r][c] + that.table[r][c];
			}
		}
		return sum;
	}
	
	/**
	 * @param that
	 * @return
	 */
	public void sum(Matrix that) {
		if (this.numRow != that.numRow || this.numCol != that.numCol) {
			throw new IllegalStateException("matrices should be of same size for sum");
		}
		
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				this.table[r][c] +=  that.table[r][c];
			}
		}
	}	

	/**
	 * @param factor
	 * @return
	 */
	public Matrix multiply(double factor) {
		Matrix multiplied = new Matrix(numRow, numCol);
		for (int r = 0; r < numRow; ++r) {
			for (int c = 0; c < numCol; ++c) {
				multiplied.table[r][c] = table[r][c] * factor;
			}
		}
		return multiplied;
	}
	
}
