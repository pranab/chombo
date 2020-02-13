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


package org.chombo.math;

import java.util.Arrays;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.chombo.util.BasicUtils;
import org.chombo.util.Pair;

import Jama.Matrix;

/**
 * Math utilities
 * @author pranab
 *
 */
public class MathUtils {
	
	//student t distribution with infinite sample size
	public static final double[][] tDistr = new double[][] {
		{0.900, 1.282},
		{0.950, 1.645},
		{0.975, 1.960},
		{0.990, 2.326},
		{0.995, 2.576}
	};

	/**
	 * @param data
	 * @return
	 */
	public static double sum(double[] data) {
		double sum = 0;
		for (double d : data){
			sum += d;
		}
		return sum;
	}

	/**
	 * @param data
	 * @return
	 */
	public static double average(double[] data) {
		double sum = 0;
		for (double d : data){
			sum += d;
		}
		return sum / data.length;
	}
	
	/**
	 * @param data
	 * @param weight
	 * @return
	 */
	public static double weightedAverage(double[] data, double[] weight) {
		double sum = 0;
		double sumWt = 0;
		for (int i = 0; i < data.length; ++i){
			sum += data[i] * weight[i];
			sumWt += weight[i];
		}
		return sum / sumWt;
	}

	/**
	 * @param data
	 * @return
	 */
	public static double max(double[] data) {
		double max = Double.MIN_VALUE;
		for (double d : data){
			if (d > max) {
				max = d;
			}
		}
		return max;
	}

	/**
	 * @param data
	 * @return
	 */
	public static double min(double[] data) {
		double min = Double.MAX_VALUE;
		for (double d : data){
			if (d < min) {
				min = d;
			}
		}
		return min;
	}

	/**
	 * @param data
	 * @param divider
	 */
	public static void divide(double[] data, double divider) {
		for (int i = 0; i < data.length; ++i) {
			data[i] /= divider;
		}
	}
	
	/**
	 * @param data
	 * @param divider
	 */
	public static void mutiply(double[] data, double multiplier) {
		for (int i = 0; i < data.length; ++i) {
			data[i] *= multiplier;
		}
	}

	/**
     * @param cons
     * @param val
     * @return
     */
    public static double expScale(double cons, double val) {
    	BasicUtils.assertCondition(cons > 0, "wrong parameter values for piecewise exponential scaling");
    	double e = Math.exp(cons * val);
    	return (e - 1) / e;
    }
    
    
    /**
     * @param xCutoff
     * @param yCutoff
     * @param consOne
     * @param consTwo
     * @param xVal
     * @return
     */
    public static double pieceWiseExpScale(double xCutoff, double yCutoff, double consOne, 
    	double consTwo, double xVal) {
    	BasicUtils.assertCondition(yCutoff < 1.0 && consOne > 0 && consTwo > 0, 
    			"wrong parameter values for piecewise exponential scaling");
    	double yVal = 0;
    	if (xVal < xCutoff) {
    		double e = Math.exp(consOne * xVal);
    		yVal =  (e - 1) / e;
    		yVal *= yCutoff;
    	} else {
    		double xValOffset = xVal - xCutoff;
    		double e = Math.exp(consTwo * xValOffset);
    		yVal =  (e - 1) / e;
    		yVal = yCutoff + yVal * (1.0 - yCutoff);
    	}
    	return yVal;
    }
    
    
    /**
     * @param cons
     * @param val
     * @return
     */
    public static double logisticScale(double cons, double val) {
    	double e = Math.exp(-cons * val);
    	return 1 / (1 + e);
    }


    /**
     * @param table
     * @param x
     * @return
     */
    public static double linearInterpolate(double[][] table, double x) {
    	return linearInterpolate(table,  x, false);
    }   
    
    /**
     * @param table
     * @param x
     * @param withinRange
     * @return
     */
    public static double linearInterpolate(double[][] table, double x, boolean withinRange) {
    	double y = 0;
    	boolean found = false;
    	int numRows = table.length;
    	
    	for (int r = 0; r < numRows; ++r) {
    		if (r < numRows - 1) {
	    		double[] cRow = table[r];
	    		double[] nRow = table[r+1];
	    		if (x > cRow[0] && x <= nRow[0]) {
	    			y = cRow[1] + (nRow[1] - cRow[1]) / (nRow[0] - cRow[0]);
	    			found = true;
	    			break;
	    		}
    		}
    	}
    	
    	//outside range
    	if (!found) {
    		if (withinRange) {
    			throw new IllegalStateException("can not interplotate outside range");
    		} else {
    			if (x <= table[0][0]) {
    				//use smallest
    				y = table[0][1];
    			} else {
    				//use largest
    				y = table[numRows-1][1];
    			}
    		}
    	}
    	return y;
    }
    
    /**
     * @param data
     * @return
     */
    public static Pair<Double, Double> linearRegression(double[] data) {
    	//index data
    	double[][] table = indexArray(data);
    	return linearRegression(table);
    }
    
    /**
     * @param data
     * @return
     */
    public static double[][] indexArray(double[] data) {
    	//index data
    	int size = data.length;
    	double[][] table = new double[size][2];
    	for (int i = 0; i < size; ++i) {
    		table[i][0] = i;
    		table[i][1] = data[i];
    	}
    	return table;
    }
    
    
    /**
     * @param table
     * @return
     */
    public static Pair<Double, Double> linearRegression(double[][] table) {
    	SimpleRegression regressor = new SimpleRegression(true);
    	regressor.addData(table);
    	Pair<Double, Double> coeff = new Pair<Double, Double>(regressor.getSlope(), regressor.getIntercept());
    	return coeff;
    }
    

    /**
     * @param data
     * @param weights
     * @return
     */
    public static Pair<Double, Double> weightedLinearRegression(double[] data, double[] weights) {
    	//index data
    	double[][] table = indexArray(data);
    	return weightedLinearRegression(table, weights);
    }

    /**
     * @param table
     * @param weights
     * @return
     */
    public static Pair<Double, Double> weightedLinearRegression(double[][] table, double[] weights) {
    	int count = table.length;
    	double wtSum = sum(weights);
    	
    	double avX = 0;
    	double avY = 0;
    	for (int i = 0; i < count; ++i) {
    		avX += weights[i] * table[i][0];
    		avY += weights[i] * table[i][1];
    	}
    	avX /= wtSum;
    	avY /= wtSum;
    	
    	double s1 = 0;
    	double s2 = 0;
    	for (int i = 0; i < count; ++i) {
    		double diffX = table[i][0] - avX;
    		double diffY = table[i][1] - avY;
    		s1 += weights[i] * (diffX * diffY);
    		s2 += weights[i] * (diffX * diffX);
    	}
    	double b1 = s1 / s2;
    	double b0 = avY - b1 * avX;
    	Pair<Double, Double> coeff = new Pair<Double, Double>(b1, b0);
    	return coeff;
    }
    
    /**
     * @param coeffs
     * @param x
     * @return
     */
    public static double linearRegressionPrediction(Pair<Double, Double> coeffs, double x) {
    	double y = coeffs.getLeft() * x + coeffs.getRight();
    	return y;
    }
    
    /**
     * @param data
     * @return
     */
    public static Matrix createMatrix(double[][] data) {
    	return new Matrix(data);
    }
    
    /**
     * @param data
     * @return
     */
    public static  double[][] invertMatrix(double[][] data) {
    	Matrix source = new Matrix(data);
    	double[][] inverted = source.inverse().getArray();
    	return inverted;
    }
    
    /**
     * @param data
     * @return
     */
    public static  Matrix invertMatrix(Matrix data) {
    	return data.inverse();
    }

    /**
     * @param data
     * @param numRows
     * @return
     */
    public static Matrix createColMatrix(double[] data, int numRows) {
    	Matrix m = null;
    	if (data.length == numRows) {
    		m = new Matrix(data, numRows);
    	} else {
    		throw new IllegalStateException("num of rows is not equal to array size");
    	}
    	return m;
    }
    
    /**
     * @param data
     * @return
     */
    public static Matrix createColMatrix(double[] data) {
    	int numRows = data.length;
        return new Matrix(data, numRows);
    }

    /**
     * @param data
     * @param numCols
     * @return
     */
    public static Matrix createRowMatrix(double[] data, int numCols) {
    	Matrix m = null;
    	if (data.length == numCols) {
    		m = new Matrix(data, 1);
    	} else {
    		throw new IllegalStateException("num of columns is not equal to array size");
    	}
    	return m;
    }
    
    /**
     * @param data
     * @param numCols
     * @return
     */
    public static Matrix createRowMatrix(double[] data) {
    	return  new Matrix(data, 1);
    }

    /**
     * @param a
     * @param b
     * @return
     */
    public static Matrix multiplyMatrix(Matrix a, Matrix b) {
    	return a.times(b);
    }

    /**
     * @param a
     * @param b
     * @return
     */
    public static double[][] multiplyMatrix(double[][] a, double[][] b) {
    	Matrix am = new Matrix(a);
    	Matrix bm = new Matrix(b);
    	Matrix c = am.times(bm);
    	return c.getArray();
    }
    
    /**
     * @param m
     * @return
     */
    public static double[] arrayFromColumnMatrix(Matrix m) {
    	double[][] data = m.getArray();
    	BasicUtils.assertCondition(data[0].length == 1, "not a column matrix");
    	int nRows = data.length;
    	double[] ar = new double[nRows];
    	for (int i = 0; i < nRows; ++i) {
    		ar[i] = data[i][0];
    	}
    	return ar;
    }

    /**
     * @param m
     * @return
     */
    public static double[] arrayFromRowMatrix(Matrix m) {
    	double[][] data = m.getArray();
    	BasicUtils.assertCondition(data.length == 1, "not a row matrix");
    	int nCols = data[0].length;
    	double[] ar = new double[nCols];
    	for (int j = 0; j < nCols; ++j) {
    		ar[j] = data[0][j];
    	}
    	return ar;
    }

    /**
     * @param m
     * @return
     */
    public static double scalarFromMatrix(Matrix m) {
    	double[][] data = m.getArray();
    	BasicUtils.assertCondition(data.length == 1 && data[0].length == 1, "not a scalar matrix");
    	return data[0][0];
    }
    
    /**
     * @param a
     * @return
     */
    public static Matrix transposeMatrix(Matrix a) {
    	return a.transpose();
    }
    
    /**
     * @param a
     * @return
     */
    public static double[][] transposeMatrix(double[][] a) {
    	Matrix am = new Matrix(a);
    	Matrix c = am.transpose();
    	return c.getArray();
    }
    
    /**
     * @param a
     * @param b
     * @return
     */
    public static Matrix addMatrix(Matrix a, Matrix b) {
    	return a.plus(b);
    }
    
    /**
     * @param a
     * @param b
     * @return
     */
    public static Matrix subtractMatrix(Matrix a, Matrix b) {
    	return a.minus(b);
    }
    
    /**
     * @param number
     * @return
     */
    public static boolean isPowerOfTwo(int value) { 
    	//power of 2 is 1 followed by by bunch of 0 and  a number 1 less is 0 followed by bunch of 1
    	boolean isPower = false;
        if (value > 0) {
        	isPower = value == 1? true :  ((value & (value - 1)) == 0);
        }
        return isPower;
    }  
    
    /**
     * @param value
     * @return
     */
    public static int binaryPowerFloor(int value) {
    	int shifted = 1;
    	while(shifted < value) {
    		shifted <<= 1;
    	}
    	if (shifted > value){
    		shifted >>= 1;
    	}
    	return shifted;
    }

    /**
     * @param value
     * @return
     */
    public static int binaryPowerCeiling(int value) {
    	int shifted = 1;
    	while(shifted < value) {
    		shifted <<= 1;
    	}
    	return shifted;
    }
    
    /**
     * @param vec
     * @return
     */
    public static double getNorm(double[] vec) {
    	RealVector rv = new ArrayRealVector(vec);
    	return rv.getNorm();
    }
    
    /**
     * @param a
     * @param b
     * @return
     */
    public static double[] subtractVector(double[] a, double[] b) {
    	RealVector va = new ArrayRealVector(a);
    	RealVector vb = new ArrayRealVector(b);
    	RealVector vc = va.subtract(vb);
    	return vc.toArray();
    }
    
    /**
     * @param a
     * @param b
     * @return
     */
    public static double vectorDiffNorm(double[] a, double[] b) {
    	RealVector va = new ArrayRealVector(a);
    	RealVector vb = new ArrayRealVector(b);
    	RealVector vc = va.subtract(vb);
    	return vc.getNorm();
    }

    /**
     * @param vec
     * @return
     */
    public static double getSum(double[] vec) {
    	double sum = 0;
    	for (double va : vec) {
    		sum += va;
    	}
    	return sum;
    }
    
    /**
     * @param vec
     * @return
     */
    public static double getAverage(double[] vec) {
    	return getSum(vec) / vec.length;
    }
    
    /**
     * @param vec
     * @param absVec
     * @return
     */
    public static void getAbsolute(double[] vec, double[] absVec) {
    	for (int i = 0; i < vec.length; ++i) {
    		absVec[i] = Math.abs(vec[i]);
    	}
    }
 
    /**
     * @param vec
     */
    public static void getAbsolute(double[] vec) {
    	for (int i = 0; i < vec.length; ++i) {
    		vec[i] = Math.abs(vec[i]);
    	}
    }
    
    /**
     * @param size
     * @return
     */
    public static double[] getZeroFilledArray(int size) {
    	return getFilledArray(size, 0.0);
    }
    
    /**
     * @param size
     * @return
     */
    public static double[] getOneFilledArray(int size) {
    	return getFilledArray(size, 1.0);
    }

    /**
     * @param size
     * @param value
     * @return
     */
    public static double[] getFilledArray(int size, double value) {
    	double[] ar = new double[size];
    	for (int i = 0; i < size; ++i) {
    		ar[i] = value;
    	}
    	return ar;
    }

    /**
     * @param vec
     * @return
     */
    public static double getMedian(double[] vec) {
    	double med = 0;
    	Arrays.sort(vec);
    	int size = vec.length;
    	int half = size / 2;
    	if (size % 2 == 1) {
    		med = vec[half];
    	} else {
    		med = (vec[half - 1] + vec[half]) / 2;
    	}
    	return med;
    }
    
    /**
     * @param data
     * @param ref
     */
    public static double[]  loessWeight(double[] data, double ref) {
    	int size = data.length;
    	double[] weights = new double[size];
    	BasicUtils.assertCondition(ref >= data[0] && ref <= data[size-1], "refrence point outside range");
    	double max = BasicUtils.max(ref - data[0], data[size-1] - ref);
    	for (int i = 0; i < size; ++i) {
    		double diff = Math.abs(ref - data[i]) / max;
    		double wt = (1.0 - Math.pow(diff, 3));
    		wt = Math.pow(wt, 3);
    		weights[i] = wt;
    	}
    	return weights;
    }
    
    /**
     * @param data
     * @param ref
     * @param neighbor
     * @return
     */
    public static int[] findNeighbors(double[] data, int ref, double[] neighbor) {
    	int size = data.length;
    	int nSize  = neighbor.length;
    	int beg = 0;
    	int refWithin = 0;
    	if (ref < nSize/2) {
    		beg = 0;
    		refWithin = ref;
    	} else if (ref > size -1 - nSize/2) {
    		beg = size - nSize;
    		refWithin = ref - beg;
    	} else {
    		beg = ref - nSize/2;
    		refWithin = nSize/2;
    	}
    	System.arraycopy(data, beg, neighbor, 0, nSize);
    	int[] result = new int[2];
    	result[0] = beg;
    	result[1] = refWithin;
    	return result;
    }
    
    /**
     * @param data
     * @param neighborSize
     */
    public static void loessSmooth(double[] data, int neighborSize) {
       double[] neighbor = new double[neighborSize];
       double[] index = createIndex(neighborSize);
       for (int i = 0; i < data.length; ++i) {
         int localRef = findNeighbors(data, i, neighbor)[1];
         double[] weights = loessWeight(index, localRef);
         Pair<Double, Double> coeffs = weightedLinearRegression(neighbor, weights);
         data[i] =   linearRegressionPrediction(coeffs, localRef);
       }
    }
    
    /**
     * @param data
     * @param neighborSize
     * @param dWeights
     */
    public static void loessSmooth(double[] data, int neighborSize, double[] dWeights) {
        double[] neighbor = new double[neighborSize];
        double[] index = createIndex(neighborSize);
        for (int i = 0; i < data.length; ++i) {
          int[] result = findNeighbors(data, i, neighbor);
          int beg = result[0];
          int localRef = result[1];
          double[] weights = loessWeight(index, localRef);
          for (int j = 0; j < neighborSize; ++j) {
        	  weights[j] *=  dWeights[beg+j];
          }
          Pair<Double, Double> coeffs = weightedLinearRegression(neighbor, weights);
          data[i] =   linearRegressionPrediction(coeffs, localRef);
        }
     }

    /**
     * @param size
     * @return
     */
    public static double[] createIndex(int size) {
    	double[] index = new double[size];
    	for (int i = 0; i < size; ++i) {
    		index[i] = i;
    	}
    	return index;
    }
    
    /**
     * @param x
     * @return
     */
    public static double biSquare(double x) {
    	double bs = 0;
    	if (x < 1.0) {
    		bs = Math.pow((1 - x * x), 2);
    	} 
    	return bs;
    }
    
    /**
     * @param x
     * @return
     */
    public static double triCube(double x) {
    	double tc = 0;
    	if (x < 1.0) {
    		tc = Math.pow((1 - x * x * x), 3);
    	} 
    	return tc;
    }
    
    /**
     * @param values
     * @return
     */
    public static Pair<Integer, Double> getMaxDiff(double[] values) {
    	double maxDiff = 0;
    	int loc = 0;
    	for (int i = 1; i < values.length; ++i) {
    		double diff = values[i] - values[i];
    		if (Math.abs(diff) > Math.abs(maxDiff)) {
    			maxDiff = diff;
    			loc = i;
    		}
    	}
    	return new Pair<Integer, Double>(loc, maxDiff);
    }
    
    /**
     * @param values
     * @return
     */
    public static Pair<Integer, Double> getMaxSecondDiff(double[] values) {
    	double maxSecDiff = 0;
    	int loc = 0;
    	for (int i = 1; i < values.length - 1; ++i) {
    		double secDiff = values[i+1] - 2 * values[i] + values[i-1];
    		if (Math.abs(secDiff) > Math.abs(maxSecDiff)) {
    			maxSecDiff = secDiff;
    			loc = i;
    		}
    	}
    	return new Pair<Integer, Double>(loc, maxSecDiff);
    }

    /**
     * @param data
     * @param winLen
     * @return
     */
    public static double[] movingAverage(double[] data, int winLen, boolean includeAnchor) {
    	double weights[] = getOneFilledArray(winLen);
    	return movingAverage(data, weights, includeAnchor);
    }
   
    /**
     * @param data
     * @param weights
     * @return
     */
    public static double[] movingAverage(double[] data, double[] weights, boolean includeAnchor) {
    	double[] averaged = null;
    	int dataLen = data.length;
    	int winLen = weights.length;
    	BasicUtils.assertCondition(winLen % 2 == 1, "winsow size should be odd");
    	int halfLength = weights.length  / 2;
    	for (int i = 0; i < dataLen; ++i) {
    		int offset = 0;
    		int winOffset = 0;
    		if (i >= halfLength) {
    			//normal and right boundary
    			offset = i - halfLength;
    			winOffset = 0;
    		} else {
    			//left boundary
    			offset = 0;
    			winOffset = halfLength - i;
    		}
    		
    		//convolution
    		double sum = 0;
    		double sumWt = 0;
    		for (int j = offset, k = winOffset; j < dataLen && k < winLen; ++j, ++k) {
    			if (includeAnchor || i != j) {
    				sum += data[j] * weights[k];
    				sumWt += weights[k];
    			}
    		}
    		averaged[i] = sum / sumWt;
    	}
    	
    	return averaged;
    }
}
