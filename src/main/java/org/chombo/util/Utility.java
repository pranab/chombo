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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

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
	private static final String PROP_FILE_EXT = ".properties";
	
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
        	if (confFilePath.startsWith(HDFS_PREFIX)) {
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
        FileSystem dfs = FileSystem.get(conf);
        Path src = new Path(filePath);
        FSDataInputStream fs = dfs.open(src);
        return fs;
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
     * @param items
     * @param fields
     * @param delim
     * @return
     */
    public static String extractFields(String[] items , int[] fields, String delim) {
    	StringBuilder stBld = new StringBuilder();
    	for (int i = 0; i < fields.length; ++i) {
    		if (i  == 0) {
    			stBld.append(items[fields[i]]);
    		} else {
    			stBld.append(delim).append(items[fields[i]]);
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
    		if (! ArrayUtils.contains(filteredFields, i)) {
    			extractedFields[j++] = items[i];
    		}
    	}
    	return extractedFields;
    }
    
}
