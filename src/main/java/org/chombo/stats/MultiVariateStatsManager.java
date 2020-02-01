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

package org.chombo.stats;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.chombo.math.MathUtils;
import org.chombo.util.BasicUtils;
import org.chombo.util.Utility;

import Jama.Matrix;


/**
 * Loads multivariate stats from file
 * @author pranab
 *
 */
public class MultiVariateStatsManager implements Serializable {
	private Map<String, Matrix> keyedMeanVec = new HashMap<String, Matrix>();
	private Map<String, Matrix> keyedCoVarMatrix = new HashMap<String, Matrix>();
	private Map<String, Matrix> keyedInvCoVarMatrix = new HashMap<String, Matrix>();
	
	/**
	 * @param statsFilePath
	 * @param delim
	 * @param fromFilePath
	 * @throws IOException
	 */
	public MultiVariateStatsManager(String statsFilePath, String delim, boolean fromHdfsFilePath) 
			throws IOException {
		InputStream fs = null;
		if (fromHdfsFilePath) {
			fs = Utility.getFileStream(statsFilePath);
		} else {
			fs = BasicUtils.getFileStream(statsFilePath);
		}
	    initialize(fs, delim);
	}
	
	/**
	 * @param fs
	 * @param delim
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	private void initialize(InputStream fs, String delim) throws NumberFormatException, IOException {
    	BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
    	String line = null; 
    	
    	String key = null;
    	String meanVecStr = null;
    	double[] meanVec = null;
    	double[][] coVarMatrix = null;
    	int coVarCounter = 0;
    	int dimension = 0;
    	while((line = reader.readLine()) != null) {
    		if (null == key) {
    			key = line;
    		} else if (null == meanVecStr) {
    			meanVec = BasicUtils.doubleArrayFromString(line, delim);
    			dimension = meanVec.length;
    			coVarMatrix = new double[dimension][dimension];
    		} else {
    			if (coVarCounter < dimension) {
    				double[] coVarRow = BasicUtils.doubleArrayFromString(line, delim);
    				coVarMatrix[coVarCounter] = coVarRow;
    				++coVarCounter;
    			} else {
    				//finish up for this key
    				keyedMeanVec.put(key, MathUtils.createRowMatrix(meanVec));
    				keyedCoVarMatrix.put(key, MathUtils.createMatrix(coVarMatrix));
    		    	key = null;
    		    	meanVecStr = null;
    		    	coVarCounter = 0;
    			}
    		}
    	}
	}
	
	/**
	 * 
	 */
	public void invertCoVarMatrix() {
		for (String key : keyedCoVarMatrix.keySet()) {
			Matrix coVarMatrix = keyedCoVarMatrix.get(key);
			keyedInvCoVarMatrix.put(key, MathUtils.invertMatrix(coVarMatrix));
		}		
	}

	/**
	 * @param key
	 * @return
	 */
	public Matrix getMeanVec(String key) {
		return keyedMeanVec.get(key);
	}

	/**
	 * @param key
	 * @return
	 */
	public Matrix getInvCoVarMatrix(String key) {
		Matrix invCoVarMatrix = keyedInvCoVarMatrix.get(key);
		if (null == invCoVarMatrix) {
			Matrix coVarMatrix = keyedCoVarMatrix.get(key);
			invCoVarMatrix = MathUtils.invertMatrix(coVarMatrix);
			keyedInvCoVarMatrix.put(key, invCoVarMatrix);
		}
		return invCoVarMatrix;
	}
	
	/**
	 * @param key
	 * @return
	 */
	public boolean statsExists(String key) {
		return null != keyedCoVarMatrix.get(key);
	}
}
