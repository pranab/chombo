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
import org.chombo.distance.AttributeDistanceSchema;
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
    	BasicUtils.initializeArray(vec, val);
    }
    
    /**
     * @param list
     * @param array
     */
    public static <T> void toList(List<T> list, T[] array) {
    	BasicUtils.toList(list, array);
    }
    
    /**
     * @param map
     * @param itemDelim
     * @param keyDelim
     * @return
     */
    public static <K,V> String serializeMap(Map<K, V> map, String itemDelim, String keyDelim) {
    	return BasicUtils.serializeMap(map, itemDelim, keyDelim);
    }
   
    /**
     * @param data
     * @param itemDelim
     * @param keyDelim
     * @return
     */
    public static   Map<String,String> deserializeMap(String data, String itemDelim, String keyDelim) {
    	return BasicUtils.deserializeMap(data, itemDelim, keyDelim);
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
    public static OutputStream getCreateFileOutputStream(String filePath) throws IOException {
    	Configuration conf = new Configuration();
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
     * @return
     * @throws IOException
     */
    public static OutputStream getCreateFileOutputStream(Configuration conf, String filePath) throws IOException {
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
    public static PrintWriter getCreateFileWriter(Configuration conf, String pathConfig) throws IOException {
        OutputStream os = Utility.getCreateFileOutputStream(conf, pathConfig);
        PrintWriter writer = new PrintWriter(os);
        return writer;
    }

    /**
     * @param filePath
     * @return
     * @throws IOException
     */
    public static PrintWriter getCreateFileWriter(String filePath) throws IOException {
        OutputStream os = Utility.getCreateFileOutputStream(filePath);
        PrintWriter writer = new PrintWriter(os);
        return writer;
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
     * @param conf
     * @param filePathParam
     * @return
     * @throws IOException
     */
    public static List<String> assertFileLines(Configuration conf, String filePathParam, String msg) throws IOException {
    	List<String> lines =  getFileLines( conf,  filePathParam);
    	if (lines.isEmpty()) {
    		throw new IllegalStateException(msg);
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
    	return BasicUtils.tokenize(text, analyzer);
    }
    
    /**
     * @param data
     * @return
     */
    public static String normalize(String data) {
    	return BasicUtils.normalize(data);
    }
 
    /**
     * @param record
     * @param remFieldOrdinal
     * @param delim
     * @return
     */
    public static String removeField(String record, int[] remFieldOrdinal, String delimRegex, String delim) {
    	return BasicUtils.removeField(record, remFieldOrdinal, delimRegex, delim);
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
    public static void appendStringTuple(String[] recordItems, int[] filterFieldOrdinal, Tuple tuple, boolean toInclude) {
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
    public static void appendStringTuple(String[] recordItems, int[] filterFieldOrdinal, Tuple tuple) {
    	appendStringTuple(recordItems, filterFieldOrdinal, tuple, true);
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
	 * @param beg
	 * @param end
	 * @param tuple
	 */
	public static void appendStringTuple(String[] record, int beg, int end, Tuple tuple) {
		for (int i = beg; i < end; ++i) {
			tuple.add(record[i]);
		}
	}

	/**
	 * @param record
	 * @param offset
	 * @param tuple
	 */
	public static void appendStringTupleFromEnd(String[] record, int offset, Tuple tuple) {
		appendStringTuple(record, offset, record.length, tuple);
	}    
	
	/**
	 * @param record
	 * @param offset
	 * @param tuple
	 */
	public static void appendStringTupleFromBegining(String[] record, int offset, Tuple tuple) {
		appendStringTuple(record, 0, offset, tuple);
	}    
	
    /** 
     * creates tuple
     * @param record coma separated  fields
     * @param tuple
     */
    public static void appendTuple(String record, Tuple tuple) {
    	String[] items = record.split(",");
    	appendStringTuple(items, 0, items.length, tuple);
    }    
	
	/**
     * @param config
     * @param param
     * @param delimRegex
     * @return
     */
    public static String[] stringArrayFromString(Configuration config, String param, String delimRegex ) {
    	return BasicUtils.stringArrayFromString(config.get(param), delimRegex);
    }
	
    /**
     * @param config
     * @param param
     * @param delimRegex
     * @return
     */
    public static int[] intArrayFromString(Configuration config, String param, String delimRegex ) {
    	return BasicUtils.intArrayFromString(config.get(param), delimRegex);
    }
    
    /**
     * @param config
     * @param param
     * @return
     */
    public static int[] intArrayFromString(Configuration config, String param ) {
    	return BasicUtils.intArrayFromString(config.get(param), DEF_FIELD_DELIM);
    }

    /**
     * @param record
     * @param delimRegex
     * @return
     */
    public static int[] intArrayFromString(String record, String delimRegex ) {
    	return BasicUtils.intArrayFromString(record, delimRegex);
    }

    /**
     * @param record
     * @return
     */
    public static int[] intArrayFromString(String record) {
    	return BasicUtils.intArrayFromString(record, DEF_FIELD_DELIM);
    }
    
    /**
     * @param record
     * @param delimRegex
     * @return
     */
    public static double[] doubleArrayFromString(String record, String delimRegex ) {
    	return BasicUtils.doubleArrayFromString(record, delimRegex);
    }
    
    /**
     * @param record
     * @return
     */
    public static double[] doubleArrayFromString(String record) {
    	return BasicUtils.doubleArrayFromString(record, DEF_FIELD_DELIM);
    }

   
    /**
     * @param items
     * @param fields
     * @return
     */
    public static String[]  extractFieldsAsStringArray(String[] items , int[] fields) {
    	return BasicUtils.extractFieldsAsStringArray(items, fields);
    }
  
    /**
     * @param items
     * @param fields
     * @return
     */
    public static int[]  extractFieldsAsIntArray(String[] items , int[] fields) {
    	return BasicUtils.extractFieldsAsIntArray(items ,fields);
    }
    
    /**
     * @param items
     * @param fields
     * @param delim
     * @param sortKeyFields
     * @return
     */
    public static String extractFields(String[] items , int[] fields, String delim, boolean sortKeyFields) {
    	return BasicUtils.extractFields(items ,fields, delim, sortKeyFields);
    }

    /**
     * @param items
     * @param filteredFields
     * @return
     */
    public static String[] filterOutFields(String[] items , int[] filteredFields) {
    	return BasicUtils.filterOutFields(items , filteredFields);
    }
    
    /**
     * @param from
     * @param toBeRemoved
     * @return
     */
    public static int[] removeItems(int[] from, int[] toBeRemoved) {
    	return BasicUtils.removeItems(from, toBeRemoved);
    }
  
    /**
     * @param valueList
     * @return
     */
    public static int[] fromListToIntArray(List<Integer> valueList) {
    	return BasicUtils.fromListToIntArray(valueList);
    }
    
    /**
     * @param values
     * @return
     */
    public static List<Integer> fromIntArrayToList( int[] values) {
    	return BasicUtils.fromIntArrayToList(values);
    }

    /**
     * @param list
     * @return
     */
    public static <T> String join(List<T> list, String delim) {
    	return BasicUtils.join(list, delim);
    }
  
    /**
     * @param list
     * @param begIndex
     * @param endIndex
     * @param delim
     * @return
     */
    public static <T> String join(List<T> list, int begIndex, int endIndex, String delim) {
    	return BasicUtils.join(list, begIndex, endIndex, delim);
    }

    /**
     * @param list
     * @return
     */
    public static <T> String join(List<T> list) {
    	return BasicUtils.join(list, ",");
    }
    
    /**
     * @param arr
     * @param delim
     * @return
     */
    public static <T> String join(T[] arr, String delim) {
    	return BasicUtils.join(arr, delim);
    }

    /**
     * @param arr
     * @param begIndex
     * @param endIndex
     * @param delim
     * @return
     */
    public static <T> String join(T[] arr, int begIndex, int endIndex, String delim) {
    	return BasicUtils.join(arr, begIndex, endIndex, delim);
    }

    /**
     * @param arr
     * @param obj
     * @return
     */
    public static <T> int getIndex(T[] arr, T obj) {
    	return BasicUtils.getIndex(arr, obj);
    }
    
    /**
     * @param arr
     * @return
     */
    public static <T> String join(T[] arr) {
    	return BasicUtils.join(arr, ",");
    }

    /**
     * @param arr
     * @param begIndex
     * @param endIndex
     * @return
     */
    public static <T> String join(T[] arr, int begIndex, int endIndex) {
    	return BasicUtils.join(arr,  begIndex, endIndex, ",");
    }
    
    /**
     * @param arr
     * @param indexes
     * @param delim
     * @return
     */
    public static <T> String join(T[] arr, int[]  indexes, String delim) {
    	return BasicUtils.join(arr,  indexes, delim);
    }
    
    /**
     * @param arr
     * @param indexes
     * @return
     */
    public static <T> String join(T[] arr, int[]  indexes) {
    	return  BasicUtils.join(arr,  indexes, ",");
    }

    /**
	 * @param table
	 * @param data
	 * @param delim
	 * @param row
	 * @param numCol
	 */
	public static void deseralizeTableRow(double[][] table, String data, String delim, int row, int numCol) {
		BasicUtils.deseralizeTableRow(table, data, delim, row, numCol);
	}
	
	/**
	 * @param table
	 * @param data
	 * @param delim
	 * @param row
	 * @param numCol
	 */
	public static void deseralizeTableRow(int[][] table, String data, String delim, int row, int numCol) {
		BasicUtils.deseralizeTableRow(table, data, delim, row, numCol);
	}
	
	/**
	 * Returns sibling path
	 * @param path
	 * @param sibling
	 * @return
	 */
	public static String getSiblingPath(String path, String sibling) {
		return  BasicUtils.getSiblingPath( path, sibling);
	}
	
	/**
	 * @param data
	 * @return
	 */
	public static boolean isBlank(String data) {
		return  BasicUtils.isBlank(data);
	}

	/**
	 * @param config
	 * @param param
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<Integer, Integer>> getIntPairList(Configuration config, String param, String fieldDelim, String subFieldDelim) {
		String record = config.get(param);
		return  null != record ? BasicUtils.getIntPairList(record, fieldDelim, subFieldDelim) : null;
	}
	
	/**
	 * @param record
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<Integer, Integer>> getIntPairList(String record, String fieldDelim, String subFieldDelim) {
		return  BasicUtils.getIntPairList(record, fieldDelim, subFieldDelim);
	}
	
	/**
	 * @param config
	 * @param param
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<Integer, String>> getIntStringList(Configuration config, String param, String fieldDelim, String subFieldDelim) {
		String record = config.get(param);
		return  null != record ? BasicUtils.getIntStringList(record, fieldDelim, subFieldDelim) : null;
	}
	
	/**
	 * @param record
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<Integer, String>> getIntStringList(String record, String fieldDelim, String subFieldDelim) {
		return  BasicUtils.getIntStringList(record, fieldDelim, subFieldDelim);
	}
	

	/**
	 * @param config
	 * @param param
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<String, String>> getStringPairList(Configuration config, String param, String fieldDelim, String subFieldDelim) {
		String record = config.get(param);
		return  null != record ? BasicUtils.getStringPairList(record, fieldDelim, subFieldDelim) : null;
	}
	
	/**
	 * @param record
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<String, String>> getStringPairList(String record, String fieldDelim, String subFieldDelim) {
		return  BasicUtils.getStringPairList(record, fieldDelim, subFieldDelim);
	}
	
	/**
	 * @param config
	 * @param param
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @return
	 */
	public static List<Pair<Integer, Boolean>> getIntBooleanList(Configuration config, String param, String fieldDelim, String subFieldDelim) {
		String record = config.get(param);
		return  null != record ? BasicUtils.getIntBooleanList(record, fieldDelim, subFieldDelim) : null;
	}
	
	/**
	 * @return
	 */
	public static String generateId() {
		return  BasicUtils.generateId();
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
	   	String stParamValue =  assertStringConfigParam(config, param,  msg); 
	   	data = intArrayFromString(stParamValue, delimRegex);
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
	   	data = doubleArrayFromString(stParamValue, delimRegex);
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
	public static Map<String, Double> assertStringDoubleMapConfigParam(Configuration config, String param, String delimRegex, 
			String subFieldDelim, String msg) {
	   	String stParamValue =  assertStringConfigParam( config, param,  msg); 
		String[] items = stParamValue.split(delimRegex);
		Map<String, Double>  data = new HashMap<String, Double>() ;
		for (String item :  items) {
			String[] parts  = item.split(subFieldDelim);
			data.put(parts[0], Double.parseDouble(parts[1]));
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
	   	return BasicUtils.integerIntegerMapFromString(stParamValue, delimRegex, subFieldDelim, rangeInKey);
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
	 * @param config
	 * @param param
	 * @param delimRegex
	 * @param msg
	 * @return
	 */
	public static String[] optionalStringArrayConfigParam(Configuration config, String param, String delimRegex) {
	   	String[] data = null;
	   	String stParamValue =  config.get(param);
	   	if (null != stParamValue) {
		   	data = stParamValue.split(delimRegex);
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
	public static int[] optionalIntArrayConfigParam(Configuration config, String param, String delimRegex) {
	   	int[] data = null;
	   	String stParamValue =  config.get(param);
	   	if (null != stParamValue) {
		   	data = intArrayFromString(stParamValue, delimRegex);
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
	public static double[] optionalDoubleArrayConfigParam(Configuration config, String param, String delimRegex) {
	   	double[] data = null;
	   	String stParamValue =  config.get(param);
	   	if (null != stParamValue) {
		   	data = doubleArrayFromString(stParamValue, delimRegex);
	   	}
    	return data;
	}

	/**
	 * @param config
	 * @param param
	 * @param delimRegex
	 * @param subFieldDelim
	 * @return
	 */
	public static Map<String, Double> OptionalStringDoubleMapConfigParam(Configuration config, String param, String delimRegex, 
			String subFieldDelim) {
		Map<String, Double>  data = null;
	   	String stParamValue =  config.get(param); 
	   	if (null != stParamValue) {
			String[] items = stParamValue.split(delimRegex);
			data = new HashMap<String, Double>() ;
			for (String item :  items) {
				String[] parts  = item.split(subFieldDelim);
				data.put(parts[0], Double.parseDouble(parts[1]));
			}
	   	}
    	return data;
	}
	
	/**
	 * @param list
	 * @return
	 */
	public static <T> T selectRandom(List<T> list) {
		return BasicUtils.selectRandom(list);
	}
	
	/**
	 * @param record
	 * @param numFields
	 * @param throwEx
	 * @return
	 */
	public static boolean isFieldCountValid(String[] record, int numFields, boolean failOnInvalid) {
		return BasicUtils.isFieldCountValid(record, numFields, failOnInvalid);
	}
	
	/**
	 * @param record
	 * @param fieldDelim
	 * @param numFields
	 * @param failOnInvalid
	 * @return
	 */
	public static String[] splitFields(String record, String fieldDelim, int numFields, boolean failOnInvalid) {
		return BasicUtils.splitFields(record, fieldDelim, numFields, failOnInvalid);
	}
	
	/**
	 * @param record
	 * @param fieldDelem
	 * @param numFields
	 * @param throwEx
	 * @return
	 */
	public static String[] getFields(String record, String fieldDelem, int numFields, boolean failOnInvalid) {
		return BasicUtils.getFields(record, fieldDelem, numFields, failOnInvalid);
	}
	
	/**
	 * @param items
	 * @return
	 */
	public static boolean anyEmptyField(String[] items) {
		return BasicUtils.anyEmptyField(items);
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
         RichAttributeSchema schema = null;
		InputStream is = Utility.getFileStream(conf, pathParam);
		if (null != is) {
			ObjectMapper mapper = new ObjectMapper();
			schema = mapper.readValue(is, RichAttributeSchema.class);
		}
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
		ProcessorAttributeSchema processingSchema = null;
		InputStream is = Utility.getFileStream(conf,  pathParam);
		if (null != is) {
			ObjectMapper mapper = new ObjectMapper();
			processingSchema = mapper.readValue(is, ProcessorAttributeSchema.class);
		}
		return processingSchema;
	}
	
	/**
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static ProcessorAttributeSchema getProcessingSchema(String filePath) throws IOException {
		ProcessorAttributeSchema processingSchema = null;
		InputStream is = Utility.getFileStream(filePath);
		if (null != is) {
			ObjectMapper mapper = new ObjectMapper();
			processingSchema = mapper.readValue(is, ProcessorAttributeSchema.class);
		}
		return processingSchema;
	}

	/**
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static AttributeDistanceSchema getAttributeDistanceSchema(Configuration conf, String pathParam) throws IOException {
		AttributeDistanceSchema  attrDistSchema = null;
		InputStream is = Utility.getFileStream(conf, pathParam);
		if (null != is) {
			ObjectMapper mapper = new ObjectMapper();
			attrDistSchema = mapper.readValue(is, AttributeDistanceSchema.class);
		}
		return attrDistSchema;
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
    	return BasicUtils.selectRandomFromList(list, count);
    }
    
    /**
     * @param curList
     * @return
     */
    public static <T>  List<T> cloneList(List<T> curList) {
    	return BasicUtils.cloneList(curList);
    }
 
    /**
     * @param list
     * @param subList
     * @return
     */
    public static <T> List<T> listDifference(List<T> list, List<T> subList) {
    	return BasicUtils.listDifference(list, subList);
    }

    /**
     * @param list
     * @param maxSubListSize
     * @return
     */
    public static <T> List<List<T>>  generateSublists(List<T> list,   int maxSubListSize) {
    	return BasicUtils.generateSublists( list,  maxSubListSize);
    }   
    
    
    /**
     * generates sub lists of varying size from a list
     * @param list
     * @param subList
     * @return
     */
    public static <T> void  generateSublists(List<T> list, List<T> subList, int lastIndex, 
    	List<List<T>> subLists, int maxSubListSize) {
    	BasicUtils.generateSublists(list, subList, lastIndex, subLists, maxSubListSize);
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
    	return BasicUtils.formatDouble(val, prec);
    }
    
    /**
     * @param val
     * @param size
     * @return
     */
    public static String formatInt(int val, int size) {
    	return BasicUtils.formatInt(val, size);
    }

    /**
     * @param val
     * @param size
     * @return
     */
    public static String formatLong(long val, int size) {
    	return BasicUtils.formatLong(val, size);
    }

    /**
     * Analyzes text and return analyzed text
     * @param text
     * @return
     * @throws IOException
     */
    public static  String analyze(String text, Analyzer analyzer) throws IOException {
    	return BasicUtils.analyze(text, analyzer);
    }

    /**
     * @param dateTimeStamp
     * @param dateFormat
     * @return
     * @throws ParseException
     */
    public static long getEpochTime(String dateTimeStamp, SimpleDateFormat dateFormat) throws ParseException {
    	return BasicUtils.getEpochTime(dateTimeStamp, false, dateFormat,0);
    }

    /**
     * @param dateTimeStamp
     * @param isEpochTime
     * @param dateFormat
     * @return
     * @throws ParseException
     */
    public static long getEpochTime(String dateTimeStamp, boolean isEpochTime, SimpleDateFormat dateFormat) throws ParseException {
    	return BasicUtils.getEpochTime(dateTimeStamp, isEpochTime, dateFormat,0);
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
    	return BasicUtils.getEpochTime(dateTimeStamp, isEpochTime, dateFormat, timeZoneShiftHour);
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
    	return BasicUtils.convertTimeUnit(epochTime, timeUnit);
    }
    
    /**
     * @param thisVector
     * @param thatVector
     * @return
     */
    public static double dotProduct(double[] thisVector, double[] thatVector) {
    	return BasicUtils.dotProduct(thisVector, thatVector);
    }
   
    /**
     * @param job
     */
    public static void setTuplePairSecondarySorting(Job job) {
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);
    }
}
