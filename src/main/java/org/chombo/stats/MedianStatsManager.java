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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.chombo.util.BasicUtils;
import org.chombo.util.ConfigUtility;
import org.chombo.util.Utility;

/**
 * Loads median related stats from HDFS
 * @author pranab
 *
 */
public class MedianStatsManager {
    private Map<Integer, Double> medians = new HashMap<Integer, Double>();
    private Map<String, Map<Integer, Double>> keyedMedians = new HashMap<String, Map<Integer, Double>>();
    private Map<Integer, Double> medAbsDiv = new HashMap<Integer, Double>();
    private Map<String, Map<Integer, Double>> keyedMedAbsDiv = new HashMap<String, Map<Integer, Double>>();
	private int[] idOrdinals;

	/**
	 * @param config
	 * @param medFilePathParam
	 * @param delim
	 * @param idOrdinals
	 * @throws IOException
	 */
	public MedianStatsManager(Configuration config, String medFilePathParam,  String delim, int[] idOrdinals, 
			boolean seasonal) throws IOException {
		this.idOrdinals = idOrdinals;
		loadMedianStat(config, medFilePathParam,  delim, idOrdinals, medians, keyedMedians,  seasonal);
	}
	
	/**
	 * @param medFilePath
	 * @param delim
	 * @param idOrdinals
	 * @throws IOException
	 */
	public MedianStatsManager(String medFilePath,  String delim, int[] idOrdinals, boolean seasonal) 
			throws IOException {
		this.idOrdinals = idOrdinals;
		loadMedianStat(medFilePath,  delim, idOrdinals, medians, keyedMedians,  seasonal);
	}

	/**
	 * @param config
	 * @param medFilepathParam
	 * @param madFilePathParam
	 * @param delim
	 * @param idOrdinals
	 * @throws IOException
	 */
	public MedianStatsManager(Configuration config, String medFilePathParam, String madFilePathParam,  
		String delim, int[] idOrdinals, boolean seasonal) 
		throws IOException {
		this.idOrdinals = idOrdinals;
		loadMedianStat(config, medFilePathParam,  delim, idOrdinals, medians, keyedMedians,  seasonal);
		loadMedianStat(config, madFilePathParam,  delim, idOrdinals, medAbsDiv, keyedMedAbsDiv,  seasonal);
	}

	/**
	 * @param config
	 * @param medFilePathParam
	 * @param madFilePathParam
	 * @param fieldDelimParam
	 * @param idOrdinalsParam
	 * @param hdfsFilePram
	 * @param seasonalParam
	 * @throws IOException
	 */
	public MedianStatsManager(Map<String, Object> config, String medFilePath, String madFilePath,  
			String fieldDelim, int[] idOrdinals, boolean hdfsFile, boolean seasonal) 
			throws IOException {
		this.idOrdinals = idOrdinals;
		loadMedianStat(medFilePath,  fieldDelim, idOrdinals, medians, keyedMedians,  hdfsFile,  seasonal);
		loadMedianStat(madFilePath,  fieldDelim, idOrdinals, medAbsDiv, keyedMedAbsDiv,  hdfsFile,  seasonal);
	}
	
	
	/**
	 * @param config
	 * @param medFilePathParam
	 * @param fieldDelimParam
	 * @param idOrdinalsParam
	 * @param hdfsFilePram
	 * @param seasonalParam
	 * @throws IOException
	 */
	public MedianStatsManager(Map<String, Object> config, String medFilePath,   
			String fieldDelim, int[] idOrdinals, boolean hdfsFile, boolean seasonal) 
			throws IOException {
		this.idOrdinals = idOrdinals;
		loadMedianStat(medFilePath,  fieldDelim, idOrdinals, medians, keyedMedians,  hdfsFile,  seasonal);
	}

	/**
	 * @param medFilePath
	 * @param madFilePath
	 * @param delim
	 * @param idOrdinals
	 * @param useFilePath
	 * @throws IOException
	 */
	public MedianStatsManager(String medFilePath, String madFilePath,  String delim, int[] idOrdinals,
			boolean useFilePath, boolean seasonal) 
			throws IOException {
		this.idOrdinals = idOrdinals;
		loadMedianStat(medFilePath,  delim, idOrdinals, medians, keyedMedians,  seasonal);
		loadMedianStat(madFilePath,  delim, idOrdinals, medAbsDiv, keyedMedAbsDiv,  seasonal);
	}

	/**
	 * @param config
	 * @param statFilePathParam
	 * @param delim
	 * @param idOrdinals
	 * @param stats
	 * @param keyedStats
	 * @throws IOException
	 */
	private void loadMedianStat(Configuration config, String statFilePathParam,   String delim, int[] idOrdinals, 
			Map<Integer, Double> stats, Map<String, Map<Integer, Double>> keyedStats, boolean seasonal) throws IOException {
		List<String> lines = Utility.getFileLines(config, statFilePathParam);
		loadMedianStat(lines, delim, stats, keyedStats,  seasonal);
	}

	/**
	 * @param config
	 * @param statFilePathParam
	 * @param delim
	 * @param idOrdinals
	 * @param stats
	 * @param keyedStats
	 * @throws IOException
	 */
	private void loadMedianStat(String statFilePath,  String delim, int[] idOrdinals, 
			Map<Integer, Double> stats, Map<String, Map<Integer, Double>> keyedStats, boolean hdfsFile, boolean seasonal) 
			throws IOException {
		List<String> lines  = null;
		if (hdfsFile) {
			lines = Utility.getFileLines(statFilePath);
		} else {
			lines  = BasicUtils.getFileLines(statFilePath);
		}
		loadMedianStat(lines, delim, stats, keyedStats, seasonal);
	}

	/**
	 * @param statFilePath
	 * @param delim
	 * @param idOrdinals
	 * @param stats
	 * @param keyedStats
	 * @throws IOException
	 */
	private void loadMedianStat(String statFilePath,   String delim, int[] idOrdinals, 
			Map<Integer, Double> stats, Map<String, Map<Integer, Double>> keyedStats, boolean seasonal) throws IOException {
		List<String> lines = Utility.getFileLines(statFilePath);
		loadMedianStat(lines, delim, stats,  keyedStats,  seasonal);
	}
	
	/**
	 * @param lines
	 * @param delim
	 * @param stats
	 * @param keyedStats
	 */
	private void loadMedianStat(List<String> lines, String delim, Map<Integer, Double> stats, Map<String, 
			Map<Integer, Double>> keyedStats, boolean seasonal) {
		for (String line : lines) {
			String[] items = line.split(delim);
			if (null != idOrdinals) {
				//with IDs
				String compId = Utility.join(items, 0, idOrdinals.length, delim);
				int i = idOrdinals.length;
	    		if (seasonal) {
	    			compId = compId + delim + items[i] + delim + items[i+1];
	    			i += 2;
	    		}
				
				Map<Integer, Double> medians = keyedStats.get(compId);
				if (null == medians) {
					medians = new HashMap<Integer, Double>();
					keyedStats.put(compId, medians);
				}
				medians.put(Integer.parseInt(items[i]), Double.parseDouble(items[i + 1]));
			} else {
				//without IDs
				stats.put(Integer.parseInt(items[0]), Double.parseDouble(items[1]));
			}
		}
		
	}
	/**
	 * @param config
	 * @param medContent
	 * @param madContent
	 * @param delim
	 * @param idOrdinals
	 * @param statInString
	 * @throws IOException
	 */
	public MedianStatsManager( String medContent, String madContent,  String delim, int[] idOrdinals) 
			throws IOException {
			loadMedianStatContent(medContent,  delim, idOrdinals, medians, keyedMedians);
			loadMedianStatContent( madContent,  delim, idOrdinals, medAbsDiv, keyedMedAbsDiv);
			this.idOrdinals = idOrdinals;
	}

	/**
	 * @param config
	 * @param statContent
	 * @param delim
	 * @param idOrdinals
	 * @param stats
	 * @param keyedStats
	 */
	private void loadMedianStatContent( String statContent,   String delim, int[] idOrdinals, 
			Map<Integer, Double> stats, Map<String, Map<Integer, Double>> keyedStats) {
    	String line = null; 
    	String[] items = null;
    	
		Scanner scanner = new Scanner(statContent);
		while (scanner.hasNextLine()) {
			line = scanner.nextLine();
    		items = line.split(delim);
			if (null != idOrdinals) {
				//with IDs
				String compId = Utility.join(items, 0, idOrdinals.length, delim);
				Map<Integer, Double> medians = keyedStats.get(compId);
				if (null == medians) {
					medians = new HashMap<Integer, Double>();
					keyedStats.put(compId, medians);
				}
				medians.put(Integer.parseInt(items[idOrdinals.length]), Double.parseDouble(items[idOrdinals.length + 1]));
			} else {
				//without IDs
				stats.put(Integer.parseInt(items[0]), Double.parseDouble(items[1]));
			}
		}		
	}
	
	/**
	 * @param attribute
	 * @return
	 */
	public double getMedian(int attribute) {
		return medians.get(attribute);
	}

	/**
	 * @param attribute
	 * @return
	 */
	public double getMedAbsDivergence(int attribute) {
		return medAbsDiv.get(attribute);
	}
	
	/**
	 * @param key
	 * @param attribute
	 * @return
	 */
	public double getKeyedMedian(String key, int attribute) {
		return keyedMedians.get(key).get(attribute);
	}
	
	/**
	 * @param key
	 * @param attribute
	 * @return
	 */
	public double getKeyedMedAbsDivergence(String key, int attribute) {
		return keyedMedAbsDiv.get(key).get(attribute);
	}

	public int[] getIdOrdinals() {
		return idOrdinals;
	}
	
	
}
