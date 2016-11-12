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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.codehaus.jackson.map.ObjectMapper;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * Generic Utility
 * @author pranab
 *
 */
public class Utility {
	private static final String CONF_FILE_PROP_NAME = "conf.path";
	private static final String FS_DEF_CONFIG_DIR = "/var/mawazo/";
	private static final String HDFS_DEF_CONFIG_DIR = "/var/mawazo/";
	private static final String HDFS_PREFIX = "hdfs:";
	private static final int HDFS_PREFIX_LEN = 5;
	private static final String S3_PREFIX = "s3n:";
	private static final String PROP_FILE_EXT = ".properties";
	
	public static final Integer ZERO = 0;
	public  static final Integer ONE = 1;
	
	public static final String DEF_FIELD_DELIM = ",";
	
	private static Pattern s3pattern = Pattern.compile("s3n:/+([^/]+)/+(.*)");
	public static String configDelim = ",";
	public  static String configSubFieldDelim = ":";
	
	public static long MILISEC_PER_HOUR = 60L * 1000 * 1000;
	public static long MILISEC_PER_HALF_DAY = 12 * MILISEC_PER_HOUR;
	public static long MILISEC_PER_DAY = 24 * MILISEC_PER_HOUR;

    /**
     * sets configuration
     * @param conf
     * @throws Exception
     */
    public static void setConfiguration(Configuration conf) throws Exception{
        String confFilePath = conf.get("conf.path");
        if (null != confFilePath){
            FileInputStream fis = new FileInputStream(confFilePath);
            Properties configProps = new Properties();
            configProps.load(fis);

            for (Object key : configProps.keySet()){
                String keySt = key.toString();
                conf.set(keySt, configProps.getProperty(keySt));
            }
        }
    }

    /**
     * sets configuration and defaults to project name based config file
     * @param conf
     * @param project
     * @throws Exception
     */
    public static void setConfiguration(Configuration conf, String project) throws Exception{
        boolean found = false;
    	String confFilePath = conf.get(CONF_FILE_PROP_NAME);
    	
    	//user provided config file path
        if (null != confFilePath){
            if (confFilePath.startsWith(S3_PREFIX)) { 
            	loadConfigS3(conf, confFilePath);
		        System.out.println("config found in user specified Amazon S3 file");
            } else if (confFilePath.startsWith(HDFS_PREFIX)) {
		        loadConfigHdfs( conf,  confFilePath.substring(HDFS_PREFIX_LEN));
		        System.out.println("config found in user specified HDFS file");
        	} else {
        		loadConfig( conf,  confFilePath, false);
		        System.out.println("config found in user specified FS  file");
        	}
         } else {
	        //default file system path
	        confFilePath = FS_DEF_CONFIG_DIR + project + PROP_FILE_EXT;
	        found = loadConfig( conf,  confFilePath, true);
	        
	        //default HDFS path
	        if (!found) {
		        confFilePath = HDFS_DEF_CONFIG_DIR + project + PROP_FILE_EXT;
		        loadConfigHdfs( conf,  confFilePath);
		        System.out.println("config found in default HDFS location");
	        }  else {
		        System.out.println("config found in default FS location");
	        }
         }
    }
    
   /**
    * @param conf
 	* @param confFilePath
 	* @param handleErr
 	* @return
 	* @throws IOException
 	*/
    private static boolean loadConfig(Configuration conf, String confFilePath, boolean handleErr ) throws IOException {
	   boolean found = false;
	   try {
	        FileInputStream fis = new FileInputStream(confFilePath);
	        Properties configProps = new Properties();
	        configProps.load(fis);
	
	        for (Object key : configProps.keySet()){
	            String keySt = key.toString();
	            conf.set(keySt, configProps.getProperty(keySt));
	        }
	        found = true;
	   } catch (FileNotFoundException ex) {
		   if (!handleErr) {
			   throw ex;
		   }
	   }
	   return found;
   }
   
   /**
    * @param conf
 	* @param confFilePath
 	* @return
 	* @throws IOException
 	*/
	private static boolean loadConfigHdfs(Configuration conf, String confFilePath) throws IOException {
	   boolean found = false;

	   FileSystem dfs = FileSystem.get(conf);
       Path src = new Path(confFilePath);
       FSDataInputStream fis = dfs.open(src);
       Properties configProps = new Properties();
       configProps.load(fis);

       for (Object key : configProps.keySet()){
           String keySt = key.toString();
           conf.set(keySt, configProps.getProperty(keySt));
       }
       found = true;
       return found;
   }

	private static boolean loadConfigS3(Configuration conf, String confFilePath) throws IOException {
        Matcher matcher = s3pattern.matcher(confFilePath);
        matcher.matches();
        String bucket = matcher.group(1);
        String key = matcher.group(2);
		AmazonS3 s3 = new AmazonS3Client(new PropertiesCredentials(Utility.class.getResourceAsStream("AwsCredentials.properties")));
        
        S3Object object = s3.getObject(new GetObjectRequest(bucket, key));
        InputStream is = object.getObjectContent();
        Properties configProps = new Properties();
        configProps.load(is);

        for (Object keyObj : configProps.keySet()){
            String keySt = keyObj.toString();
            conf.set(keySt, configProps.getProperty(keySt));
        }
        return true;
	}	
	
    /**
     * sets configuration and defaults to project name based config file
     * @param conf
     * @param project
     * @param filterByGroup
     * @throws Exception
     */
    public static void setConfiguration(Configuration conf, String project, boolean filterByGroup) throws Exception{
    	if (filterByGroup) {
    		ConfigurationLoader configLoader = new ConfigurationLoader(conf, project);
    		configLoader.set();
    	} else {
    		setConfiguration(conf, project);
    	}
    }
    
    /**
     * @param vec
     * @param val
     */
    public static <T> void initializeArray(T[] vec, T val)  {
    	for(int i = 0; i < vec.length; ++i) {
    		vec[i] = val;
    	}
    }
    
    /**
     * @param list
     * @param array
     */
    public static <T> void toList(List<T> list, T[] array) {
    	for (T val : array) {
    		list.add(val);
    	}
    }
    
    /**
     * @param map
     * @param itemDelim
     * @param keyDelim
     * @return
     */
    public static <K,V> String serializeMap(Map<K, V> map, String itemDelim, String keyDelim) {
    	StringBuilder stBld = new StringBuilder();
    	for (K key : map.keySet()) {
    		stBld.append(key).append(keyDelim).append(map.get(key)).append(itemDelim);
    	}
    	return stBld.substring(0, stBld.length() -1);    
    }
   
    /**
     * @param data
     * @param itemDelim
     * @param keyDelim
     * @return
     */
    public static   Map<String,String> deserializeMap(String data, String itemDelim, String keyDelim) {
    	Map<String,String> map = new HashMap<String,String>();
    	String[] items = data.split(itemDelim);
    	for (String item : items) {
    		String[] fields = item.split(keyDelim) ;
    		map.put(fields[0], fields[1]);
    	}
    	return map;
    }
    
    /**
     * @param conf
     * @param pathConfig
     * @return
     * @throws IOException
     */
    public static InputStream getFileStream(Configuration conf, String pathConfig) throws IOException {
        String filePath = conf.get(pathConfig);
        FSDataInputStream fs = null;
        if (null != filePath) {
        	FileSystem dfs = FileSystem.get(conf);
        	Path src = new Path(filePath);
        	fs = dfs.open(src);
        }
        return fs;
    }
    
    /**
     * @param conf
     * @param pathConfig
     * @return
     * @throws IOException
     */
    public static InputStream getFileStream(String filePath) throws IOException {
    	Configuration conf = new Configuration();
        FSDataInputStream fs = null;
        if (null != filePath) {
        	FileSystem dfs = FileSystem.get(conf);
        	Path src = new Path(filePath);
        	fs = dfs.open(src);
        }
        return fs;
    }
 
    /**
     * @param conf
     * @param pathConfig
     * @return
     * @throws IOException
     */
    public static OutputStream getCreateFileOutputStream(Configuration conf, String pathConfig) throws IOException {
        String filePath = conf.get(pathConfig);
        FSDataOutputStream fs = null;
        if (null != filePath) {
        	FileSystem dfs = FileSystem.get(conf);
        	Path src = new Path(filePath);
        	fs = dfs.create(src, true);
        }
        return fs;
    }

    /**
     * @param conf
     * @param pathConfig
     * @param data
     * @throws IOException
     */
    public static void writeToFile(Configuration conf, String pathConfig, String data) throws IOException {
        OutputStream os = Utility.getCreateFileOutputStream(conf, pathConfig);
        PrintWriter writer = new PrintWriter(os);
		writer.write(data);
		writer.close();
		os.close();
    }

    /**
     * @param conf
     * @param pathConfig
     * @return
     * @throws IOException
     */
    public static OutputStream getAppendFileOutputStream(Configuration conf, String pathConfig) throws IOException {
        String filePath = conf.get(pathConfig);
        FSDataOutputStream fs = null;
        if (null != filePath) {
        	FileSystem dfs = FileSystem.get(conf);
        	Path src = new Path(filePath);
        	fs = dfs.append(src);
        }
        return fs;
    }

    /**
     * @param conf
     * @param pathConfig
     * @param data
     * @throws IOException
     */
    public static void appendToFile(Configuration conf, String pathConfig, String data) throws IOException {
        OutputStream os = Utility.getAppendFileOutputStream(conf, pathConfig);
        PrintWriter writer = new PrintWriter(os);
		writer.write(data);
		writer.close();
		os.close();
    }
    
    /**
     * @param conf
     * @param filePathParam
     * @param fieldDelimRegex
     * @return
     * @throws IOException
     */
    public static List<String[]> parseFileLines(Configuration conf, String filePathParam, String fieldDelimRegex) throws IOException {
    	List<String[]> lines = new ArrayList<String[]>();
    	InputStream fs = getFileStream(conf, filePathParam);
    	if (null != fs) {
    		BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
    		String line = null; 
    		String[] items = null;
    	
    		while((line = reader.readLine()) != null) {
    			items = line.split(fieldDelimRegex);
    			lines.add(items);
    		}
    	}
    	return lines;
    }
    
    /**
     * @param conf
     * @param filePathParam
     * @return
     * @throws IOException
     */
    public static List<String> getFileLines(Configuration conf, String filePathParam) throws IOException {
    	List<String> lines = new ArrayList<String>();
    	InputStream fs = getFileStream(conf, filePathParam);
    	if (null != fs) {
    		BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
    		String line = null; 
    		while((line = reader.readLine()) != null) {
    			lines.add(line);
    		}
    	}
    	return lines;
    }

    /**
     * @param filePath
     * @return
     * @throws IOException
     */
    public static List<String> getFileLines(String filePath) throws IOException {
    	List<String> lines = new ArrayList<String>();
    	InputStream fs = getFileStream(filePath);
    	if (null != fs) {
    		BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
    		String line = null; 
    		while((line = reader.readLine()) != null) {
    			lines.add(line);
    		}
    	}
    	return lines;
    }

    /**
     * @param text
     * @param analyzer
     * @return
     * @throws IOException
     */
    public static List<String> tokenize(String text, Analyzer analyzer) throws IOException {
        TokenStream stream = analyzer.tokenStream("contents", new StringReader(text));
        List<String> tokens = new ArrayList<String>();

        CharTermAttribute termAttribute = (CharTermAttribute)stream.getAttribute(CharTermAttribute.class);
        while (stream.incrementToken()) {
    		String token = termAttribute.toString();
    		tokens.add(token);
    	} 
    	
    	return tokens;
    }
    
    /**
     * @param data
     * @return
     */
    public static String normalize(String data) {
    	String[] items = data.toLowerCase().split("\\s+");
    	return items.length > 0 ? StringUtils.join(items, " ") : items[0];
    }
 
    /**
     * @param record
     * @param remFieldOrdinal
     * @param delim
     * @return
     */
    public static String removeField(String record, int[] remFieldOrdinal, String delimRegex, String delim) {
    	StringBuilder stBld = new StringBuilder();
    	String[] items = record.split(delimRegex);
    	boolean first = true;
    	for (int i = 0; i < items.length; ++i) {
    		if (!ArrayUtils.contains(remFieldOrdinal, i)) {
    			if (first) {
    				stBld.append(items[i]);
    				first = false;
    			} else {
    				stBld.append(delim).append(items[i]);
    			}
    		}
    	}
    	return stBld.toString();
    }

    /** creates tuple
     * @param recordItems record  fields
     * @param remFieldOrdinal record fields to be excluded
     * @return
     */
    public static void createTuple(String[] recordItems, int[] remFieldOrdinal, Tuple tuple) {
    	tuple.initialize();
    	for (int i = 0; i < recordItems.length; ++i) {
    		if (!ArrayUtils.contains(remFieldOrdinal, i)) {
    			tuple.add(recordItems[i]);
    		}
    	}
    }    
    
    /**
     * @param recordItems
     * @param filterFieldOrdinal
     * @param tuple
     * @param toInclude
     */
    public static void createStringTuple(String[] recordItems, int[] filterFieldOrdinal, Tuple tuple, boolean toInclude) {
    	tuple.initialize();
    	for (int i = 0; i < recordItems.length; ++i) {
    		if (!toInclude && !ArrayUtils.contains(filterFieldOrdinal, i)  || toInclude && ArrayUtils.contains(filterFieldOrdinal, i)) {
    			tuple.add(recordItems[i]);
    		}
    	}
    }    

    /**
     * @param recordItems
     * @param filterFieldOrdinal
     * @param tuple
     */
    public static void createStringTuple(String[] recordItems, int[] filterFieldOrdinal, Tuple tuple) {
    	createStringTuple(recordItems, filterFieldOrdinal, tuple, true);
    }    

    /**
     * @param recordItems
     * @param filterFieldOrdinal
     * @param tuple
     * @param toInclude
     */
    public static void createIntTuple(String[] recordItems, int[] filterFieldOrdinal, Tuple tuple, boolean toInclude) {
    	tuple.initialize();
    	for (int i = 0; i < recordItems.length; ++i) {
    		if (!toInclude && !ArrayUtils.contains(filterFieldOrdinal, i)  || toInclude && ArrayUtils.contains(filterFieldOrdinal, i)) {
    			tuple.add(Integer.parseInt(recordItems[i]));
    		}
    	}
    }    
    
    /**
     * @param recordItems
     * @param filterFieldOrdinal
     * @param tuple
     */
    public static void createIntTuple(String[] recordItems, int[] filterFieldOrdinal, Tuple tuple) {
    	createIntTuple(recordItems, filterFieldOrdinal, tuple, true);
    }    

    /** creates tuple
     * @param record coma separated  fields
     * @param tuple
     */
    public static void createTuple(String  record, Tuple tuple) {
    	String[] items = record.split(",");
    	createStringTuple(items, 0, items.length, tuple);
    }    
 
	/**
	 * @param record
	 * @param offset
	 * @param tuple
	 */
	public static void createStringTupleFromBegining(String[] record, int offset, Tuple tuple) {
		createStringTuple(record, 0, offset, tuple);
	}    

	/**
	 * @param record
	 * @param offset
	 * @param tuple
	 */
	public static void createStringTupleFromEnd(String[] record, int offset, Tuple tuple) {
		createStringTuple(record, offset, record.length, tuple);
	}    

	/**
	 * @param record
	 * @param beg
	 * @param end
	 * @param tuple
	 */
	public static void createStringTuple(String[] record, int beg, int end, Tuple tuple) {
		tuple.initialize();
		for (int i = beg; i < end; ++i) {
			tuple.add(record[i]);
		}
	}
    
    /**
     * @param record
     * @param delimRegex
     * @return
     */
    public static int[] intArrayFromString(String record, String delimRegex ) {
    	int[] data = null;
    	if (null != record) {
	    	String[] items = record.split(delimRegex);
	    	data = new int[items.length];
	    	for (int i = 0; i < items.length; ++i) {
	    		data[i] = Integer.parseInt(items[i]);
	    	}
    	}
    	return data;
    }

    /**
     * @param record
     * @return
     */
    public static int[] intArrayFromString(String record) {
    	return intArrayFromString(record, DEF_FIELD_DELIM);
    }
    
    /**
     * @param record
     * @param delimRegex
     * @return
     */
    public static double[] doubleArrayFromString(String record, String delimRegex ) {
    	String[] items = record.split(delimRegex);
    	double[] data = new double[items.length];
    	for (int i = 0; i < items.length; ++i) {
    		data[i] = Double.parseDouble(items[i]);
    	}
    	return data;
    }
    
    /**
     * @param record
     * @return
     */
    public static double[] doubleArrayFromString(String record) {
    	return doubleArrayFromString(record, DEF_FIELD_DELIM);
    }

    /**
     * @param items
     * @param fields
     * @return
     */
    public static String[]  extractFieldsAsStringArray(String[] items , int[] fields) {
    	String[] fieldValues = new String[fields.length];
    	for (int i = 0; i < fields.length; ++i) {
    		fieldValues[i] = items[fields[i]];
    	}
    	return fieldValues;
    }
  
    /**
     * @param items
     * @param fields
     * @return
     */
    public static int[]  extractFieldsAsIntArray(String[] items , int[] fields) {
    	int[] fieldValues = new int[fields.length];
    	for (int i = 0; i < fields.length; ++i) {
    		fieldValues[i] = Integer.parseInt((items[fields[i]]));
    	}
    	return fieldValues;
    }
    
    /**
     * @param items
     * @param fields
     * @param delim
     * @param sortKeyFields
     * @return
     */
    public static String extractFields(String[] items , int[] fields, String delim, boolean sortKeyFields) {
    	StringBuilder stBld = new StringBuilder();
    	List<String> keyFields = new ArrayList<String>();
    	
    	for (int i = 0; i < fields.length; ++i) {
    		keyFields.add(items[fields[i]]);
    	}

    	if  (sortKeyFields) {
    		Collections.sort(keyFields);
    	}
    	
    	boolean first = true;
    	for (String key : keyFields) {
    		if (first) {
    			stBld.append(key);
    			first = false;
    		} else {
    			stBld.append(delim).append(key);
    		}
    	}
    	return stBld.toString();
    }

    /**
     * @param items
     * @param filteredFields
     * @return
     */
    public static String[] filterOutFields(String[] items , int[] filteredFields) {
    	String[] extractedFields = new String[items.length - filteredFields.length ]; 
    	
    	for (int i = 0, j=0; i < items.length; ++i) {
    		if (!ArrayUtils.contains(filteredFields, i)) {
    			extractedFields[j++] = items[i];
    		}
    	}
    	return extractedFields;
    }
    
    /**
     * @param from
     * @param toBeRemoved
     * @return
     */
    public static  int[] removeItems(int[] from, int[] toBeRemoved) {
    	int[] subtracted = null;
    	List<Integer> subtractedList = new ArrayList<Integer>();
    	
    	for (int i = 0; i < from.length; ++i) {
    		int item = from[i];
    		if (!ArrayUtils.contains(toBeRemoved, item)) {
    			subtractedList.add(item);
    		}
    	}
    	subtracted = fromListToIntArray(subtractedList);
    	return subtracted;
    }
  
    /**
     * @param valueList
     * @return
     */
    public static int[] fromListToIntArray(List<Integer> valueList) {
		int[] values = new int[valueList.size()];
		for (int i = 0; i < valueList.size(); ++i) {
			values[i] = valueList.get(i);
		}
		return values;
    }
    
    /**
     * @param values
     * @return
     */
    public static List<Integer> fromIntArrayToList( int[] values) {
    	List<Integer> valueList  = new  ArrayList<Integer>();  
		for (int value :  values) {
			valueList.add(value);;
		}
		return valueList;
    }

    /**
     * @param list
     * @return
     */
    public static <T> String join(List<T> list, String delim) {
    	String joined = null;
    	if (list.size() == 1) {
    		joined = list.get(0).toString();
    	} else {
	    	StringBuilder stBld = new StringBuilder();
	    	for (T obj : list) {
	    		stBld.append(obj).append(delim);
	    	}
	    	
	    	joined =  stBld.substring(0, stBld.length() -1);
    	}
    	return joined;
    }
  
    /**
     * @param list
     * @param begIndex
     * @param endIndex
     * @param delim
     * @return
     */
    public static <T> String join(List<T> list, int begIndex, int endIndex, String delim) {
    	StringBuilder stBld = new StringBuilder();
    	for (int i = begIndex; i < endIndex; ++i) {
    		stBld.append(list.get(i)).append(delim);
    	}
    	return stBld.substring(0, stBld.length() -1);
    }

    /**
     * @param list
     * @return
     */
    public static <T> String join(List<T> list) {
    	return join(list, ",");
    }
    
    /**
     * @param arr
     * @param delim
     * @return
     */
    public static <T> String join(T[] arr, String delim) {
    	StringBuilder stBld = new StringBuilder();
    	for (T obj : arr) {
    		stBld.append(obj).append(delim);
    	}
    	
    	return stBld.substring(0, stBld.length() -1);
    }

    /**
     * @param arr
     * @param begIndex
     * @param endIndex
     * @param delim
     * @return
     */
    public static <T> String join(T[] arr, int begIndex, int endIndex, String delim) {
    	StringBuilder stBld = new StringBuilder();
    	for (int i = begIndex; i < endIndex; ++i) {
    		stBld.append(arr[i]).append(delim);
    	}
    	
    	return stBld.substring(0, stBld.length() -1);
    }

    /**
     * @param arr
     * @param obj
     * @return
     */
    public static <T> int getIndex(T[] arr, T obj) {
    	int i = 0;
    	boolean found = false;
    	for (T thisObj : arr) {
    		if (thisObj.equals(obj)) {
    			found = true;
    			break;
    		}
    		++i;
    	}
    	if (!found) {
    		throw new IllegalArgumentException("object not found in array");
    	}
    	return i;
    }
    
    /**
     * @param arr
     * @return
     */
    public static <T> String join(T[] arr) {
    	return join(arr, ",");
    }

    /**
     * @param arr
     * @param begIndex
     * @param endIndex
     * @return
     */
    public static <T> String join(T[] arr, int begIndex, int endIndex) {
    	return join(arr,  begIndex, endIndex, ",");
    }
    
    /**
     * @param arr
     * @param indexes
     * @param delim
     * @return
     */
    public static <T> String join(T[] arr, int[]  indexes, String delim) {
    	StringBuilder stBld = new StringBuilder();
    	for (int index  : indexes) {
    		stBld.append(arr[index]).append(delim);
    	}
    	return stBld.substring(0, stBld.length() -1);
    }
    
    /**
     * @param arr
     * @param indexes
     * @return
     */
    public static <T> String join(T[] arr, int[]  indexes) {
    	return  join(arr,  indexes, ",");
    }

    /**
	 * @param table
	 * @param data
	 * @param delim
	 * @param row
	 * @param numCol
	 */
	public static void deseralizeTableRow(double[][] table, String data, String delim, int row, int numCol) {
		String[] items = data.split(delim);
		if (items.length != numCol) {
			throw new IllegalArgumentException(
					"Row serialization failed, number of tokens in string does not match with number of columns");
		}
		for (int c = 0; c < numCol; ++c) {
				table[row][c]  = Double.parseDouble(items[c]);
		}
	}
	
	/**
	 * @param table
	 * @param data
	 * @param delim
	 * @param row
	 * @param numCol
	 */
	public static void deseralizeTableRow(int[][] table, String data, String delim, int row, int numCol) {
		String[] items = data.split(delim);
		int k = 0;
		for (int c = 0; c < numCol; ++c) {
				table[row][c]  = Integer.parseInt(items[k++]);
		}
	}
	
	/**
	 * Returns sibling path
	 * @param path
	 * @param sibling
	 * @return
	 */
	public static String getSiblingPath(String path, String sibling) {
		int pos = path.lastIndexOf('/');
		return path.substring(0, pos + 1) + sibling;
	}
	
	/**
	 * @param data
	 * @return
	 */
	public static boolean isBlank(String data) {
		return data == null || data.isEmpty();
	}
	
	/**
	 * @param record
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<Integer, Integer>> getIntPairList(String record, String fieldDelim, String subFieldDelim) {
		List<Pair<Integer, Integer>> intPairs = new ArrayList<Pair<Integer, Integer>>();
		String[] items = record.split(fieldDelim);
		for (String item : items) {
			String[] subItems = item.split(subFieldDelim);
			Pair<Integer, Integer> pair = new Pair<Integer, Integer>(Integer.parseInt(subItems[0]),  Integer.parseInt(subItems[1]));
			intPairs.add(pair);
		}
		return intPairs;
	}
	
	/**
	 * @param record
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<Integer, String>> getIntStringList(String record, String fieldDelim, String subFieldDelim) {
		List<Pair<Integer, String>> intStringPairs = new ArrayList<Pair<Integer, String>>();
		String[] items = record.split(fieldDelim);
		for (String item : items) {
			String[] subItems = item.split(subFieldDelim);
			Pair<Integer, String> pair = new Pair<Integer, String>(Integer.parseInt(subItems[0]),  subItems[1]);
			intStringPairs.add(pair);
		}
		return intStringPairs;
	}
	
	/**
	 * @param record
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<String, String>> getStringPairList(String record, String fieldDelim, String subFieldDelim) {
		List<Pair<String, String>> stringStringPairs = new ArrayList<Pair<String, String>>();
		String[] items = record.split(fieldDelim);
		for (String item : items) {
			String[] subItems = item.split(subFieldDelim);
			Pair<String, String> pair = new Pair<String, String>(subItems[0],  subItems[1]);
			stringStringPairs.add(pair);
		}
		return stringStringPairs;
	}
	
	/**
	 * @return
	 */
	public static String generateId() {
		return UUID.randomUUID().toString().replaceAll("-", "");
	}
	
	/**
	 * @param config
	 * @param param
	 * @param msg
	 */
	public static String  assertConfigParam(Configuration config, String param, String msg) {
		return assertStringConfigParam( config,param, msg);
	}

	/**
	 * @param config
	 * @param param
	 * @param msg
	 * @return
	 */
	public static String  assertStringConfigParam(Configuration config, String param, String msg) {
		String  value = config.get(param);
		if (value == null) {
			throw new IllegalStateException(msg);
		} 
		return value;
	}
	
	/**
	 * @param config
	 * @param param
	 * @param msg
	 * @return
	 */
	public static int  assertIntConfigParam(Configuration config, String param, String msg) {
		int  value = Integer.MIN_VALUE;
		assertStringConfigParam( config, param,  msg); 
		value = config.getInt(param,  Integer.MIN_VALUE);
		return value;
	}

	/**
	 * @param config
	 * @param param
	 * @param msg
	 * @return
	 */
	public static double  assertDoubleConfigParam(Configuration config, String param, String msg) {
		double  value = Double.MIN_VALUE;
		String stParamValue = assertStringConfigParam(config, param,  msg); 
		value = Double.parseDouble(stParamValue);
		return value;
	}

	/**
	 * @param config
	 * @param param
	 * @param msg
	 * @return
	 */
	public static boolean  assertBooleanConfigParam(Configuration config, String param, String msg) {
		boolean value = false;
	   	assertStringConfigParam(config, param,  msg); 
		value = config.getBoolean(param, false);
		return value;
	}

	/**
	 * @param config
	 * @param param
	 * @param delimRegex
	 * @param msg
	 * @return
	 */
	public static int[] assertIntArrayConfigParam(Configuration config, String param, String delimRegex, String msg) {
	   	int[] data = null;
	   	String stParamValue =  assertStringConfigParam( config, param,  msg); 
		String[] items = stParamValue.split(delimRegex);
		data = new int[items.length];
		for (int i = 0; i < items.length; ++i) {
			data[i] = Integer.parseInt(items[i]);
		}
    	return data;
	}
	
	/**
	 * @param config
	 * @param param
	 * @param delimRegex
	 * @param msg
	 * @return
	 */
	public static String[] assertStringArrayConfigParam(Configuration config, String param, String delimRegex, String msg) {
	   	String stParamValue =  assertStringConfigParam( config, param,  msg); 
		return  stParamValue.split(delimRegex);
	}

	/**
	 * @param config
	 * @param param
	 * @param delimRegex
	 * @param msg
	 * @return
	 */
	public static double[] assertDoubleArrayConfigParam(Configuration config, String param, String delimRegex, String msg) {
	   	double[] data = null;
	   	String stParamValue =  assertStringConfigParam( config, param,  msg); 
		String[] items = stParamValue.split(delimRegex);
		data = new double[items.length];
		for (int i = 0; i < items.length; ++i) {
			data[i] = Double.parseDouble(items[i]);
		}
    	return data;
	}

	/**
	 * @param config
	 * @param param
	 * @param delimRegex
	 * @param subFieldDelim
	 * @param msg
	 * @return
	 */
	public static Map<String, Integer> assertStringIntegerMapConfigParam(Configuration config, String param, String delimRegex, 
			String subFieldDelim, String msg) {
	   	String stParamValue =  assertStringConfigParam( config, param,  msg); 
		String[] items = stParamValue.split(delimRegex);
		Map<String, Integer>  data = new HashMap<String, Integer>() ;
		for (String item :  items) {
			String[] parts  = item.split(subFieldDelim);
			data.put(parts[0], Integer.parseInt(parts[1]));
		}
    	return data;
	}

	/**
	 * @param config
	 * @param param
	 * @param delimRegex
	 * @param subFieldDelim
	 * @param msg
	 * @return
	 */
	public static Map<Integer, Integer> assertIntIntegerIntegerMapConfigParam(Configuration config, String param, String delimRegex, 
			String subFieldDelim, String msg) {
		return assertIntegerIntegerMapConfigParam(config, param, delimRegex, subFieldDelim, msg, true);
	}

	/**
	 * @param config
	 * @param param
	 * @param delimRegex
	 * @param subFieldDelim
	 * @param msg
	 * @param rangeInKey
	 * @return
	 */
	public static Map<Integer, Integer> assertIntegerIntegerMapConfigParam(Configuration config, String param, String delimRegex, 
			String subFieldDelim, String msg, boolean rangeInKey) {
	   	String stParamValue =  assertStringConfigParam( config, param,  msg); 
		String[] items = stParamValue.split(delimRegex);
		Map<Integer, Integer> data = new HashMap<Integer, Integer>() ;
		if (rangeInKey) {
			for (String item :  items) {
				String[] parts  = item.split(subFieldDelim);
				String[] rangeLimits = parts[0].split("\\-");
				if (rangeLimits.length == 1) {
					data.put(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
				} else {
					int rangeBeg = Integer.parseInt(rangeLimits[0]);
					int rangeEnd = Integer.parseInt(rangeLimits[1]);
					int val = Integer.parseInt(parts[1]);
					for (int r = rangeBeg; r <= rangeEnd; ++r) {
						//key:hour value:hour group
						data.put(r,  val);
					}
				}
			}
		} else {
			for (String item :  items) {
				String[] parts  = item.split(subFieldDelim);
				data.put(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
			}
		}
    	return data;
	}

	
	/**
	 * @param config
	 * @param param
	 * @param delimRegex
	 * @param subFieldDelim
	 * @param msg
	 * @return
	 */
	public static Map<Integer, Double> assertIntegerDoubleMapConfigParam(Configuration config, String param, String delimRegex, 
			String subFieldDelim, String msg) {
	   	String stParamValue =  assertStringConfigParam( config, param,  msg); 
		String[] items = stParamValue.split(delimRegex);
		Map<Integer, Double> data = new HashMap<Integer, Double>() ;
		for (String item :  items) {
			String[] parts  = item.split(subFieldDelim);
			data.put(Integer.parseInt(parts[0]), Double.parseDouble(parts[1]));
		}
    	return data;
	}

	/**
	 * @param config
	 * @param param
	 * @param delimRegex
	 * @param subFieldDelim
	 * @param msg
	 * @return
	 */
	public static Map<Integer, String> assertIntegerStringMapConfigParam(Configuration config, String param, String delimRegex, 
			String subFieldDelim, String msg) {
	   	String stParamValue =  assertStringConfigParam(config, param,  msg); 
		String[] items = stParamValue.split(delimRegex);
		Map<Integer, String> data = new HashMap<Integer, String>() ;
		for (String item :  items) {
			String[] parts  = item.split(subFieldDelim);
			data.put(Integer.parseInt(parts[0]), parts[1]);
		}
    	return data;
	}

	/**
	 * @param config
	 * @param param
	 * @param delimRegex
	 * @param subFieldDelim
	 * @param msg
	 * @return
	 */
	public static Map<String, Double> assertDoubleMapConfigParam(Configuration config, String param, String delimRegex, 
			String subFieldDelim, String msg) {
	   	String stParamValue =  assertStringConfigParam( config, param,  msg); 
		String[] items = stParamValue.split(delimRegex);
		Map<String, Double> data = new HashMap<String, Double>() ;
		for (String item :  items) {
			String[] parts  = item.split(subFieldDelim);
			data.put(parts[0], Double.parseDouble(parts[1]));
		}
    	return data;
	}

	/**
	 * @param record
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<String, String>> assertStringPairListConfigParam(Configuration config, String param,  
			String fieldDelim, String subFieldDelim, String msg) {
		String record = assertStringConfigParam(config, param, msg);
		return  getStringPairList(record, fieldDelim, subFieldDelim); 
	}	

	/**
	 * @param record
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<Integer, String>> assertIntStringListConfigParam(Configuration config, String param,  
			String fieldDelim, String subFieldDelim, String msg) {
		String record = assertStringConfigParam(config, param, msg);
		return  getIntStringList(record, fieldDelim, subFieldDelim); 
	}	
	
	/**
	 * @param record
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<Integer, Integer>> assertIntPairListConfigParam(Configuration config, String param,  
			String fieldDelim, String subFieldDelim, String msg) {
		String record = assertStringConfigParam(config, param, msg);
		return  getIntPairList(record, fieldDelim, subFieldDelim); 
	}	

	/**
	 * @param list
	 * @return
	 */
	public static <T> T selectRandom(List<T> list) {
   		int index = (int)(Math.random() * list.size());
		return list.get(index);
	}
	
	/**
	 * @param record
	 * @param numFields
	 * @param throwEx
	 * @return
	 */
	public static boolean isFieldCountValid(String[] record, int numFields, boolean failOnInvalid) {
		boolean valid = true;
		if (record.length != numFields) {
			valid = false;
			if (failOnInvalid) {
				throw new IllegalArgumentException("invalid field count expected " + numFields + " found " + record.length);
			}
		}
		return valid;
	}
	
	/**
	 * @param record
	 * @param fieldDelim
	 * @param numFields
	 * @param failOnInvalid
	 * @return
	 */
	public static String[] splitFields(String record, String fieldDelim, int numFields, boolean failOnInvalid) {
		String[] items = record.split(fieldDelim, -1);
		if (items.length != numFields) {
			if (items.length < numFields) {
				//check if trailing blank fields
				int delimCount = StringUtils.countMatches(record, fieldDelim);
				if (delimCount == numFields - 1) {
					//trailing blank fields
					String[] extItems = new String[numFields];
					for (int i = 0; i < numFields; ++i) {
						if (i < items.length) {
							extItems[i] = items[i];
						} else {
							//fill trailing fields with blanks
							extItems[i] = "";
						}
					}
					items = extItems;
				} else {
					//got too few fields
					items = null;
				}
			} else {
				//got too many fields
				items = null;
			}
			
			if (null == items && failOnInvalid) {
				throw new IllegalArgumentException("invalid field count expected " + numFields + " found " + items.length);
			}
		}
		return items;
	}
	
	/**
	 * @param record
	 * @param fieldDelem
	 * @param numFields
	 * @param throwEx
	 * @return
	 */
	public static String[] getFields(String record, String fieldDelem, int numFields, boolean failOnInvalid) {
		String[] fields = record.split(fieldDelem);
		if (fields.length != numFields) {
			if (failOnInvalid) {
				throw new IllegalArgumentException("invalid field count expected " + numFields + " found " + fields.length);
			}
			fields = null;
		}
		return fields;
	}
	
	/**
	 * @param items
	 * @return
	 */
	public static boolean anyEmptyField(String[] items) {
		boolean isEmpty = false;
		for (String item : items) {
			if (item.isEmpty()) {
				isEmpty = true;
				break;
			}
		}
		return isEmpty;
	}
	
	/**
	 * @param conf
	 * @param pathConfig
	 * @return Hconf config object
	 * @throws IOException
	 */
	public static Config getHoconConfig(Configuration conf, String pathConfig) throws IOException {
		Config config =  null;
		if (null  !=  conf.get(pathConfig)) {
			InputStream is = getFileStream(conf, pathConfig);
			BufferedReader bufRead =new BufferedReader(new InputStreamReader(is));
			config =  ConfigFactory.parseReader(bufRead);
		}
		return config;
	}
	
	/**
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static Config getHoconConfig(String filePath) throws IOException {
		Config config =  null;
		if (null  !=  filePath) {
			InputStream is = getFileStream(filePath);
			BufferedReader bufRead =new BufferedReader(new InputStreamReader(is));
			config =  ConfigFactory.parseReader(bufRead);
		}
		return config;
	}

	/**
	 * @param conf
	 * @param pathParam
	 * @return
	 * @throws IOException
	 */
	public static RichAttributeSchema getRichAttributeSchema(Configuration conf, String pathParam) throws IOException {
    	String filePath = conf.get(pathParam);
        FileSystem dfs = FileSystem.get(conf);
        Path src = new Path(filePath);
        FSDataInputStream fs = dfs.open(src);
        ObjectMapper mapper = new ObjectMapper();
        RichAttributeSchema schema = mapper.readValue(fs, RichAttributeSchema.class);
        return schema;
	}

	/**
	 * @param conf
	 * @param pathParam
	 * @return
	 * @throws IOException
	 */
	public static GenericAttributeSchema getGenericAttributeSchema(Configuration conf, String pathParam) throws IOException {
		GenericAttributeSchema schema = null;
		InputStream is = Utility.getFileStream(conf, pathParam);
		if (null != is) {
			ObjectMapper mapper = new ObjectMapper();
			schema = mapper.readValue(is, GenericAttributeSchema.class);
		}
		return schema;
	}
	
	/**
	 * @param conf
	 * @param pathParam
	 * @return
	 * @throws IOException
	 */
	public static FeatureSchema getFeatureSchema(Configuration conf, String pathParam) throws IOException {
		FeatureSchema schema = null;
		InputStream is = Utility.getFileStream(conf, pathParam);
		if (null != is) {
			ObjectMapper mapper = new ObjectMapper();
			schema = mapper.readValue(is, FeatureSchema.class);
		}
		return schema;
	}

	/**
	 * @param conf
	 * @param pathParam
	 * @return
	 * @throws IOException
	 */
	public static ProcessorAttributeSchema getProcessingSchema(Configuration conf, String pathParam) throws IOException {
		InputStream is = Utility.getFileStream(conf,  pathParam);
		ObjectMapper mapper = new ObjectMapper();
		ProcessorAttributeSchema processingSchema = mapper.readValue(is, ProcessorAttributeSchema.class);
		return processingSchema;
	}
	
	/**
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static ProcessorAttributeSchema getProcessingSchema( String filePath) throws IOException {
		InputStream is = Utility.getFileStream(filePath);
		ObjectMapper mapper = new ObjectMapper();
		ProcessorAttributeSchema processingSchema = mapper.readValue(is, ProcessorAttributeSchema.class);
		return processingSchema;
	}

	/**
	 * @param config
	 * @param params
	 * @return
	 */
	public static Map<String, String> collectConfiguration(Configuration config, String... params ) {
		Map<String, String> collectedConfig = new HashMap<String, String>();
		for (String param : params) {
			collectedConfig.put(param, config.get(param));
		}
		return collectedConfig;
	}

    /**
     * @param list
     * @param count
     * @return
     */
    public static <T> List<T> selectRandomFromList(List<T> list, int count) {
    	List<T> selList = null;
    	if (count > list.size()) {
    		throw new IllegalArgumentException("new list size is larget than source list size");
    	} else if (count == list.size()) {
    		selList  = list;
    	} else {
    		selList = new ArrayList<T>();
           	Set<T> selSet = new  HashSet<T>();
           	while (selSet.size() != count) {
           		int index = (int)(Math.random() * list.size());
           		selSet.add(list.get(index));
           	}
           	selList.addAll(selSet);	
    	}
    	return selList;
    }
    
    /**
     * @param curList
     * @return
     */
    public static <T>  List<T> cloneList(List<T> curList) {
    	List<T> newList = new ArrayList<T>();
    	newList.addAll(curList);
    	return newList;
    }
 
    /**
     * @param list
     * @param subList
     * @return
     */
    public static <T> List<T> listDifference(List<T> list, List<T> subList) {
    	List<T> diff = new ArrayList<T>();
    	for (T item : list) {
    		if (!subList.contains(item)) {
    			diff.add(item);
    		}
    	}
    	return diff;
    }

    /**
     * @param list
     * @param maxSubListSize
     * @return
     */
    public static <T> List<List<T>>  generateSublists(List<T> list,   int maxSubListSize) {
    	 List<List<T>> subLists = new ArrayList<List<T>>();
    	 
    	 //for each  item  in list generate sublists up to max length
    	 for (int i = 0; i < list.size();  ++i) {
    		 List<T> subList = new ArrayList<T>();
    		 subList.add(list.get(i));
    		 subLists.add(subList);
    		 generateSublists(list, subList, i, subLists, maxSubListSize);
    	 }
    	 return subLists;
    }   
    
    
    /**
     * generates sub lists of varying size from a list
     * @param list
     * @param subList
     * @return
     */
    public static <T> void  generateSublists(List<T> list, List<T> subList, int lastIndex, 
    	List<List<T>> subLists, int maxSubListSize) {
    	for (int i = lastIndex + 1; i < list.size(); ++i) {
    			List<T> biggerSubList = new ArrayList<T>();
    			biggerSubList.addAll(subList);
    			biggerSubList.add(list.get(i));
    			subLists.add(biggerSubList);
    			if (biggerSubList.size() < maxSubListSize) {
    				//recurse
    				generateSublists(list, biggerSubList, i, subLists, maxSubListSize);
    			}
    	}    	
    }
    
    /**
     * Takes user specified attributes or builds  list of attributes of right type from schema 
     * @param attrListParam
     * @param configDelim
     * @param schema
     * @param config
     * @param includeTypes
     * @return
     */
    public static int[] getAttributes(String attrListParam, String configDelim, GenericAttributeSchema schema, 
    		Configuration config, String... includeTypes) {        	
    	int[] attributes = Utility.intArrayFromString(config.get(attrListParam), configDelim);
    	List<Attribute> attrsMetaData = schema != null ? schema.getQuantAttributes(includeTypes) : null;
    	if (null == attributes) {
    		//use schema and pick all attributes of right type
    		if (null == attrsMetaData) {
    			throw new IllegalStateException("Neither attribute ordinal list ot schema available");
    		}
    		attributes = new int[attrsMetaData.size()];
    		for (int i = 0; i < attrsMetaData.size(); ++i) {
    			attributes[i] = attrsMetaData.get(i).getOrdinal();
    		}
    	} else {
    		//use user provided but verify type
    		if (null != attrsMetaData) {
    			//if schema is available
	    		for (int ord : attributes ) {
	    			boolean found = false;
	    			for (Attribute attr : attrsMetaData) {
	    				if (attr.getOrdinal() == ord) {
	    					found = true;
	    					break;
	    				}
	    			}
				
	    			if (!found) {
	    				throw new IllegalArgumentException("attribute not found in metada");
	    			}
	    		}
    		}
    	}
    	return attributes;
    }
    
    /**
     * @param record
     * @param attributes
     * @param schema
     * @param tuple
     */
    public static void intializeTuple(String[] record, int[] attributes, GenericAttributeSchema schema, Tuple tuple) {
    	tuple.initialize();
    	for (int attr : attributes) {
    		String dataType = schema.findAttribute(attr).getDataType();
    		if (dataType.equals(Attribute.DATA_TYPE_INT)) {
    			tuple.add(Integer.parseInt(record[attr]));
    		} else if (dataType.equals(Attribute.DATA_TYPE_LONG)) {
    			tuple.add(Long.parseLong(record[attr]));
    		}  else {
    			tuple.add(record[attr]);
    		}
    	}
    }


    /**
     * @param val
     * @param prec
     * @return
     */
    public static String formatDouble(double val, int prec) {
    	String formatter = "%." + prec + "f";
    	return String.format(formatter, val);
    }
    
    /**
     * @param val
     * @param size
     * @return
     */
    public static String formatInt(int val, int size) {
    	String formatter = "%0" + size + "d";
    	return String.format(formatter, val);
    }

    /**
     * @param val
     * @param size
     * @return
     */
    public static String formatLong(long val, int size) {
    	String formatter = "%0" + size + "d";
    	return String.format(formatter, val);
    }

    /**
     * Analyzes text and return analyzed text
     * @param text
     * @return
     * @throws IOException
     */
    public static  String analyze(String text, Analyzer analyzer) throws IOException {
        TokenStream stream = analyzer.tokenStream("contents", new StringReader(text));
        StringBuilder stBld = new StringBuilder();

        stream.reset();
        CharTermAttribute termAttribute = (CharTermAttribute)stream.getAttribute(CharTermAttribute.class);
        while (stream.incrementToken()) {
    		String token = termAttribute.toString();
    		stBld.append(token).append(" ");
    	} 
    	stream.end();
    	stream.close();
    	return stBld.toString();
    }

    /**
     * @param dateTimeStamp
     * @param dateFormat
     * @return
     * @throws ParseException
     */
    public static long getEpochTime(String dateTimeStamp, SimpleDateFormat dateFormat) throws ParseException {
    	return getEpochTime(dateTimeStamp, false, dateFormat,0);
    }

    /**
     * @param dateTimeStamp
     * @param isEpochTime
     * @param dateFormat
     * @return
     * @throws ParseException
     */
    public static long getEpochTime(String dateTimeStamp, boolean isEpochTime, SimpleDateFormat dateFormat) throws ParseException {
    	return getEpochTime(dateTimeStamp, isEpochTime, dateFormat,0);
    }
    
    /**
     * @param dateTimeStamp
     * @param isEpochTime
     * @param dateFormat
     * @param timeZoneShiftHour
     * @return
     * @throws ParseException
     */
    public static long getEpochTime(String dateTimeStamp, boolean isEpochTime, SimpleDateFormat dateFormat, 
    	int timeZoneShiftHour) throws ParseException {
    	long epochTime = 0;
    	if (isEpochTime) {
    		epochTime = Long.parseLong(dateTimeStamp);
    	} else {
    		epochTime = dateFormat.parse(dateTimeStamp).getTime();
        	epochTime += timeZoneShiftHour * MILISEC_PER_HOUR;
    	}
    	
    	return epochTime;
    }
    
    /**
     * @param config
     * @param fieldDelimParam
     * @param defFieldDelimParam
     * @param defFieldDelim
     * @return
     */
    public static String getFieldDelimiter(Configuration config, String fieldDelimParam, 
    		String defFieldDelimParam, String defFieldDelim) {
    	String fieldDelim = config.get(fieldDelimParam);
    	if (null == fieldDelim) {
    		//get default
    		fieldDelim = config.get(defFieldDelimParam, defFieldDelim);
    	}
    	return fieldDelim;
    }
    
    /**
     * @param epochTime
     * @param timeUnit
     * @return
     */
    public static long convertTimeUnit(long epochTime, String timeUnit) {
    	long modTime = epochTime;
		if (timeUnit.equals("hour")) {
			modTime /= MILISEC_PER_HOUR;
		} else if (timeUnit.equals("day")) {
			modTime /= MILISEC_PER_DAY;
		} else {
			throw new IllegalArgumentException("invalid time unit");
		}
    	return modTime;
    }
    
    /**
     * @param thisVector
     * @param thatVector
     * @return
     */
    public static double dotProduct(double[] thisVector, double[] thatVector) {
    	double product = 0;
    	if (thisVector.length != thatVector.length) {
    		throw new IllegalArgumentException("mismatched size for vector dot product");
    	}
    	
    	for (int i = 0; i < thisVector.length; ++i) {
    		product += thisVector[i] * thatVector[i];
    	}
    	return product;
    }
   
    /**
     * @param job
     */
    public static void setTuplePairSecondarySorting(Job job) {
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);
    }
}
