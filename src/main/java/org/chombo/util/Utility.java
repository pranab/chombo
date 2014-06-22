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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;


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
    /*
    static AmazonS3 s3 = null;
 	static {
		try {	
			s3 = new AmazonS3Client(new PropertiesCredentials(Utility.class.getResourceAsStream("AwsCredentials.properties")));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	*/
    
    
	
    /**
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
    
    /** creates tuple
     * @param record coma separated  fields
     * @param tuple
     */
    public static void createTuple(String  record, Tuple tuple) {
    	tuple.initialize();
    	String[] items = record.split(",");
    	for (String item : items) {
    		tuple.add(item);
    	}
    }    
    
    /**
     * @param record
     * @param delimRegex
     * @return
     */
    public static int[] intArrayFromString(String record, String delimRegex ) {
    	String[] items = record.split(delimRegex);
    	int[] data = new int[items.length];
    	for (int i = 0; i < items.length; ++i) {
    		data[i] = Integer.parseInt(items[i]);
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
     * @param delim
     * @return
     */
    public static String extractFields(String[] items , int[] fields, String delim, boolean sortKeyFields) {
    	StringBuilder stBld = new StringBuilder();
    	List<String> keyFields = new ArrayList();
    	
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
     * @param list
     * @return
     */
    public static <T> String join(List<T> list, String delim) {
    	StringBuilder stBld = new StringBuilder();
    	for (T obj : list) {
    		stBld.append(obj).append(delim);
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
     * @return
     */
    public static <T> String join(T[] arr) {
    	return join(arr, ",");
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
}
