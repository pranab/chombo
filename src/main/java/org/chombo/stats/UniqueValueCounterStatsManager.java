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
import java.util.Set;

import org.chombo.util.BasicUtils;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class UniqueValueCounterStatsManager implements Serializable {
	private Map<String, Map<Integer, Integer>> counts = new HashMap<String, Map<Integer, Integer>>();
	
	/**
	 * @param statsFilePath
	 * @param delim
	 * @param fromHdfsFilePath
	 * @throws IOException
	 */
	public UniqueValueCounterStatsManager(String statsFilePath, String delim, 
			int[] idOrdinals, boolean seasonal, boolean fromHdfsFilePath) throws IOException {
		InputStream fs = null;
		if (fromHdfsFilePath) {
			fs = Utility.getFileStream(statsFilePath);
		} else {
			fs = BasicUtils.getFileStream(statsFilePath);
		}
		initialize( fs,  delim,  idOrdinals,  seasonal);
	}

	/**
	 * @param fs
	 * @param delim
	 * @param idOrdinals
	 * @param seasonal
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	private void initialize(InputStream fs, String delim, int[] idOrdinals, boolean seasonal) throws NumberFormatException, IOException {
    	BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
    	String line = null; 
    	String[] items = null;
    	
		//base keys, seasonal keys, field ordinal, count
    	while((line = reader.readLine()) != null) {
    		items = line.split(delim);
    		
    		//comp key is composite IDs plus seasonal type and index
    		int i = 0;
    		String compKey = BasicUtils.join(items, 0, idOrdinals.length);
    		i  += idOrdinals.length;
    		if (seasonal) {
    			compKey = compKey + delim + items[i] + delim + items[i+1];
    			i += 2;
    		}
    		
    		//atr ordinal
    		Integer attr = Integer.parseInt(items[i++]);
    		
    		//count
    		Integer count = Integer.parseInt(items[i++]);
    		
    		Map<Integer, Integer> fieldCounts = counts.get(compKey);
    		if (null == fieldCounts) {
    			fieldCounts = new HashMap<Integer, Integer>();
    			counts.put(compKey, fieldCounts);
    		}
    		fieldCounts.put(attr, count);
    		
    	}
	}
	
	/**
	 * @return
	 */
	public Set<String> getAllKeys() {
		return counts.keySet();
	}
	
	/**
	 * @param compKey
	 * @param attr
	 * @return
	 */
	public int getCount(String compKey, int attr) {
		Map<Integer, Integer> fieldCounts = counts.get(compKey);
		return fieldCounts.get(attr);
	}

}
