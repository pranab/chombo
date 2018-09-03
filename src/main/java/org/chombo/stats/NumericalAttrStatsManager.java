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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.chombo.util.BasicUtils;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Loads numerical attribute stats from HDFS file and provides access methods
 * @author pranab
 *
 */
public class NumericalAttrStatsManager implements Serializable {
	private Map<Integer, List<Tuple>> stats = new HashMap<Integer, List<Tuple>>();
	private Map<String, Map<Integer, List<Tuple>>> keyedStats = new HashMap<String, Map<Integer, List<Tuple>>>();
	public static final String DEF_COND_ATTR_VAL = "$";
	
	/**
	 * Stats for data
	 * @param config
	 * @param statsFilePath
	 * @param delim
	 * @throws IOException
	 */
	public NumericalAttrStatsManager(Configuration config, String statsFilePathParam, String delim) 
		throws IOException {
    	InputStream fs = Utility.getFileStream(config, statsFilePathParam);
    	initialize(fs, delim);

	}

	/**
	 * @param statsFilePath
	 * @param delim
	 * @param fromFilePath
	 * @throws IOException
	 */
	public NumericalAttrStatsManager(String statsFilePath, String delim, boolean fromHdfsFilePath) 
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
    	String[] items = null;
    	
		//(0)attr ord (1)cond attr (2)sum (3)sum square (4)count (5)mean (6)variance (7)std dev (8)min (9)max 
    	while((line = reader.readLine()) != null) {
    		items = line.split(delim);
    		Tuple tuple = new Tuple();
    		int i = 0;
    		Integer attr = Integer.parseInt(items[0]);
    		tuple.add(Tuple.STRING, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		tuple.add(Tuple.INT, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		
    		List<Tuple> statList = stats.get(attr);
    		if (null ==  statList ) {
    			statList = new ArrayList<Tuple>();
    			stats.put(attr, statList );
    		}
    		statList.add( tuple);
    	}
	}
	
	/**
	 * @param statsContent
	 * @param delim
	 * @throws IOException
	 */
	public NumericalAttrStatsManager( String statsContent, String delim) 
			throws IOException {
	    	String line = null; 
	    	String[] items = null;
	    	
			Scanner scanner = new Scanner(statsContent);
			while (scanner.hasNextLine()) {
			  line = scanner.nextLine();
	    		items = line.split(delim);
	    		Tuple tuple = new Tuple();
	    		int i = 0;
	    		Integer attr = Integer.parseInt(items[0]);
	    		tuple.add(Tuple.STRING, items[i++]);
	    		tuple.add(Tuple.DOUBLE, items[i++]);
	    		tuple.add(Tuple.DOUBLE, items[i++]);
	    		tuple.add(Tuple.INT, items[i++]);
	    		tuple.add(Tuple.DOUBLE, items[i++]);
	    		tuple.add(Tuple.DOUBLE, items[i++]);
	    		tuple.add(Tuple.DOUBLE, items[i++]);
	    		
	    		List<Tuple> statList = stats.get(attr);
	    		if (null ==  statList ) {
	    			statList = new ArrayList<Tuple>();
	    			stats.put(attr, statList );
	    		}
	    		statList.add( tuple);
			}
		}

	/**
	 * Stats for keyed data
	 * @param config
	 * @param statsFilePath
	 * @param delim
	 * @param idOrdinals
	 * @throws IOException
	 */
	public NumericalAttrStatsManager(String statsFilePath, String delim, 
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
    	
		//(0)attr ord (1)cond attr (2)sum (3)sum square (4)count (5)mean (6)variance (7)std dev (8)min (9)max 
    	while((line = reader.readLine()) != null) {
    		items = line.split(delim);
    		Tuple tuple = new Tuple();
    		
    		//comp key
    		int i = 0;
    		String compKey = BasicUtils.join(items, 0, idOrdinals.length);
    		i  += idOrdinals.length;
    		if (seasonal) {
    			compKey = compKey + delim + items[i] + delim + items[i+1];
    			i += 2;
    		}
    		
    		Integer attr = Integer.parseInt(items[i++]);
    		
    		//stats
    		tuple.add(Tuple.STRING, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		tuple.add(Tuple.INT, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		
    		//add to map
    		Map<Integer, List<Tuple>> stats = keyedStats.get(compKey);
    		if (null == stats) {
    			stats = new HashMap<Integer, List<Tuple>>();
    			keyedStats.put(compKey, stats);
    		}
    		
    		//list , one for each conditional var value
    		List<Tuple> statList = stats.get(attr);
    		if (null ==  statList ) {
    			statList = new ArrayList<Tuple>();
    			stats.put(attr, statList );
    		}
    		statList.add( tuple);
    	}
		
	}	
	
	/**
	 * Stats for keyed data
	 * @param config
	 * @param statsFilePath
	 * @param delim
	 * @param idOrdinals
	 * @throws IOException
	 */
	public NumericalAttrStatsManager(Configuration config, String statsFilePath, String delim, int[] idOrdinals) 
		throws IOException {
    	InputStream fs = Utility.getFileStream(config, statsFilePath);
    	BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
    	String line = null; 
    	String[] items = null;
    	
		//(0)attr ord (1)cond attr (2)sum (3)sum square (4)count (5)mean (6)variance (7)std dev (8)min (9)max 
    	while((line = reader.readLine()) != null) {
    		items = line.split(delim);
    		Tuple tuple = new Tuple();
    		int i = 0;
    		String compKey = Utility.join(items, 0, idOrdinals.length);
    		i  += idOrdinals.length;
    		Integer attr = Integer.parseInt(items[i++]);
    		tuple.add(Tuple.STRING, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		tuple.add(Tuple.INT, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		tuple.add(Tuple.DOUBLE, items[i++]);
    		
    		//add to map
    		Map<Integer, List<Tuple>> stats = keyedStats.get(compKey);
    		if (null == stats) {
    			stats = new HashMap<Integer, List<Tuple>>();
    			keyedStats.put(compKey, stats);
    		}
    		List<Tuple> statList = stats.get(attr);
    		if (null ==  statList ) {
    			statList = new ArrayList<Tuple>();
    			stats.put(attr, statList );
    		}
    		statList.add(tuple);
    	}
	}
	
	/**
	 * @param attr
	 * @param condAttrVal
	 * @return
	 */
	private Tuple getStats(int attr, String condAttrVal) {
		Tuple foundTuple = null;
		
		//for all cond attribute values
		List<Tuple> statList = stats.get(attr);
		
		//search by cond attribute
		for (Tuple tuple : statList) {
			if (tuple.getString(0).equals(condAttrVal)) {
				foundTuple = tuple;
				break;
			}
		}
		return foundTuple;
	}
	
	/**
	 * @param attr
	 * @return
	 */
	public double getSum(int attr) {
		Tuple tuple = getStats(attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(1);
	}
	
	/**
	 * @param attr
	 * @return
	 */
	public double getSumSq(int attr) {
		Tuple tuple = getStats(attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(2);
	}
	
	/**
	 * @param attr
	 * @return
	 */
	public int getCount(int attr) {
		Tuple tuple = getStats(attr, DEF_COND_ATTR_VAL);
		return tuple.getInt(3);
	}
	
	/**
	 * @param attr
	 * @return
	 */
	public double getMean(int attr) {
		Tuple tuple = getStats(attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(4);
	}

	/**
	 * @param attr
	 * @return
	 */
	public double getVariance(int attr) {
		Tuple tuple = getStats(attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(5);
	}
	
	/**
	 * @param attr
	 * @return
	 */
	public double getStdDev(int attr) {
		Tuple tuple = getStats(attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(6);
	}
	
	/**
	 * @param attr
	 * @return
	 */
	public double getMin(int attr) {
		Tuple tuple = getStats(attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(7);
	}

	/**
	 * @param attr
	 * @return
	 */
	public double getMax(int attr) {
		Tuple tuple = getStats(attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(8);
	}

	public StatsParameters getStatsParameters(int attr) {
		StatsParameters stats = new StatsParameters();
		stats.setMean(getMean(attr));
		stats.setStdDev(getStdDev(attr));
		stats.setMin(getMin(attr));
		stats.setMin(getMax(attr));
		return stats;
	}
	
	/**
	 * @param attr
	 * @param condAttrVal
	 * @return
	 */
	private Tuple getKeyedStats(String compKey, int attr, String condAttrVal) {
		Tuple foundTuple = null;
		Map<Integer, List<Tuple>> stats = keyedStats.get(compKey);
		List<Tuple> statList = stats.get(attr);
		for (Tuple tuple : statList) {
			if (tuple.getString(0).equals(condAttrVal)) {
				foundTuple = tuple;
				break;
			}
		}
		return foundTuple;
	}

	/**
	 * @param compKey
	 * @param attr
	 * @return
	 */
	public double getSum(String compKey,int attr) {
		Tuple tuple = getKeyedStats(compKey, attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(1);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @param condAttrVal
	 * @return
	 */
	public double getSum(String compKey,int attr, String condAttrVal) {
		Tuple tuple = getKeyedStats(compKey, attr, condAttrVal);
		return tuple.getDouble(1);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @return
	 */
	public double getSumSq(String compKey, int attr) {
		Tuple tuple = getKeyedStats(compKey, attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(2);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @param condAttrVal
	 * @return
	 */
	public double getSumSq(String compKey, int attr, String condAttrVal) {
		Tuple tuple = getKeyedStats(compKey, attr, condAttrVal);
		return tuple.getDouble(2);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @return
	 */
	public int getCount(String compKey, int attr) {
		Tuple tuple = getKeyedStats(compKey, attr, DEF_COND_ATTR_VAL);
		return tuple.getInt(3);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @param condAttrVal
	 * @return
	 */
	public int getCount(String compKey, int attr, String condAttrVal) {
		Tuple tuple = getKeyedStats(compKey, attr, condAttrVal);
		return tuple.getInt(3);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @return
	 */
	public double getMean(String compKey, int attr) {
		Tuple tuple = getKeyedStats(compKey, attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(4);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @param condAttrVal
	 * @return
	 */
	public double getMean(String compKey, int attr, String condAttrVal) {
		Tuple tuple = getKeyedStats(compKey, attr, condAttrVal);
		return tuple.getDouble(4);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @return
	 */
	public double getVariance(String compKey, int attr) {
		Tuple tuple = getKeyedStats(compKey, attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(5);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @param condAttrVal
	 * @return
	 */
	public double getVariance(String compKey, int attr, String condAttrVal) {
		Tuple tuple = getKeyedStats(compKey, attr, condAttrVal);
		return tuple.getDouble(5);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @return
	 */
	public double getStdDev(String compKey, int attr) {
		Tuple tuple = getKeyedStats(compKey, attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(6);
	}
	
	/**
	 * @param compKey
	 * @param attr
	 * @param condAttrVal
	 * @return
	 */
	public double getStdDev(String compKey, int attr, String condAttrVal) {
		Tuple tuple = getKeyedStats(compKey, attr, condAttrVal);
		return tuple.getDouble(6);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @return
	 */
	public double getMin(String compKey, int attr) {
		Tuple tuple = getKeyedStats(compKey, attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(7);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @param condAttrVal
	 * @return
	 */
	public double getMin(String compKey, int attr, String condAttrVal) {
		Tuple tuple = getKeyedStats(compKey, attr, condAttrVal);
		return tuple.getDouble(7);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @return
	 */
	public double getMax(String compKey,  int attr) {
		Tuple tuple = getKeyedStats(compKey, attr, DEF_COND_ATTR_VAL);
		return tuple.getDouble(8);
	}

	/**
	 * @param compKey
	 * @param attr
	 * @param condAttrVal
	 * @return
	 */
	public double getMax(String compKey,  int attr, String condAttrVal) {
		Tuple tuple = getKeyedStats(compKey, attr, condAttrVal);
		return tuple.getDouble(8);
	}

}
