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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * Sets configuration. Global config params are always loaded. Specific config group 
 * params as identified by a key prefix are also loaded. A version can also be specified
 * for the config group being loaded. A group will correspond to configuration parameters
 * for a map reduce job
 * 
 * @author pranab
 *
 */
public class ConfigurationLoader {
	private Configuration conf;
	private String project;
	private List<String> groups = new ArrayList<String>();
	private int version;
	
	private final String CONF_FILE_PROP_NAME = "conf.path";
	private final String CONF_GROUP = "conf.group";
	
	private final String FS_DEF_CONFIG_DIR = "/var/mawazo/";
	private final String HDFS_DEF_CONFIG_DIR = "/var/mawazo/";
	private final String HDFS_PREFIX = "hdfs:";
	private final int HDFS_PREFIX_LEN = 5;
	private final String S3_PREFIX = "s3n:";
	private final String PROP_FILE_EXT = ".properties";
	private Pattern s3pattern = Pattern.compile("s3n:/+([^/]+)/+(.*)");
	private final String GLOBAL_KEY_PREFIX = "global";
	private int globalKeyPrefixLen = GLOBAL_KEY_PREFIX.length();

	
    /**
     * @param conf
     * @param project
     * @param keyPrefix
     */
    public ConfigurationLoader(Configuration conf, String project) {
		super();
		this.conf = conf;
		this.project = project;
		
		String confGroup = conf.get(CONF_GROUP);
		if (null == confGroup) {
			throw new IllegalStateException("configuration group not provided");
		}
		
		if (confGroup.contains(Utility.configDelim)) {
			//multiple groups and/or higher version
			String[] items = confGroup.split(Utility.configDelim);
			
			//group names and end with version
			for (int i = 0; i < items.length - 1; ++i) {
				groups.add(items[i]);
			}
			version = Integer.parseInt(items[items.length - 1]);
		} else {
			//only one group with default version of 0
			groups.add(confGroup);
		}
	}
	

	/**
     * @param conf
     * @param project
     * @throws Exception
     */
    public void set() throws Exception{
        boolean found = false;
    	String confFilePath = conf.get(CONF_FILE_PROP_NAME);
    	
    	//user provided config file path
        if (null != confFilePath){
            if (confFilePath.startsWith(S3_PREFIX)) { 
            	loadConfigS3(confFilePath);
		        System.out.println("config found in user specified Amazon S3 file");
            } else if (confFilePath.startsWith(HDFS_PREFIX)) {
		        loadConfigHdfs(confFilePath.substring(HDFS_PREFIX_LEN));
		        System.out.println("config found in user specified HDFS file");
        	} else {
        		loadConfig(confFilePath, false);
		        System.out.println("config found in user specified FS  file");
        	}
         } else {
	        //default file system path
	        confFilePath = FS_DEF_CONFIG_DIR + project + PROP_FILE_EXT;
	        found = loadConfig(confFilePath, true);
	        
	        //default HDFS path
	        if (!found) {
		        confFilePath = HDFS_DEF_CONFIG_DIR + project + PROP_FILE_EXT;
		        loadConfigHdfs(confFilePath);
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
    private  boolean loadConfig(String confFilePath, boolean handleErr) throws IOException {
	   boolean found = false;
	   
	   try {
	        FileInputStream fis = new FileInputStream(confFilePath);
	        List<String> lines = getConfigLines(fis);
	        setFilteredConfiguration(lines);      	
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
	private  boolean loadConfigHdfs(String confFilePath) throws IOException {
	   boolean found = false;

	   FileSystem dfs = FileSystem.get(conf);
       Path src = new Path(confFilePath);
       FSDataInputStream fis = dfs.open(src);
       List<String> lines = getConfigLines(fis);
       setFilteredConfiguration(lines);      	
       found = true;
       return found;
   }

	/**
	 * @param confFilePath
	 * @return
	 * @throws IOException
	 */
	private  boolean loadConfigS3(String confFilePath) throws IOException {
        Matcher matcher = s3pattern.matcher(confFilePath);
        matcher.matches();
        String bucket = matcher.group(1);
        String key = matcher.group(2);
		AmazonS3 s3 = new AmazonS3Client(new PropertiesCredentials(Utility.class.getResourceAsStream("AwsCredentials.properties")));
        
        S3Object object = s3.getObject(new GetObjectRequest(bucket, key));
        InputStream fis = object.getObjectContent();
        List<String> lines = getConfigLines(fis);
        setFilteredConfiguration(lines);      	
        return true;
	}	
	

	/**
	 * @param lines
	 */
	private void setFilteredConfiguration(List<String> lines) {
		Map<String, Pair<String, Integer>> multiVersionConfig = new HashMap<String, Pair<String, Integer>>();
		
		System.out.println("configuration settings:");
        for (String line : lines) {
        	if (!line.isEmpty() && !line.startsWith("#")) {
        		String[] items = line.split("=");
        		if (items.length != 2) {
        			throw new IllegalArgumentException("invalid cofig file format");
        		}
        		items[0] = items[0].trim();
        		items[1] = items[1].trim();
        		
        		String valSt = items[1];
        		if (items[0].startsWith(GLOBAL_KEY_PREFIX)) {
        			//global config
        			String keySt = items[0].substring(globalKeyPrefixLen + 1);
    	            conf.set(keySt, valSt);
    	            System.out.println(keySt + "\\t" + valSt);
        		} else if (shouldInclude(items[0])) {
        			//specific config group
        			String keySt = items[0];
        			Pair<String, Integer> versionedVal = multiVersionConfig.get(keySt);
        			if (null == versionedVal) {
        				versionedVal = new Pair<String, Integer>(valSt, -1);
        			}
        			
        			//keep replacing until we have reached the right version
        			if (versionedVal.getRight() < version) {
        				multiVersionConfig.put(keySt, new Pair<String, Integer>(valSt, versionedVal.getRight() + 1));
        			}
        		}
        	}
        }
        
        //nothing found
        if (multiVersionConfig.isEmpty()) {
        	throw new IllegalStateException("no configuration parameter found for groups " + groups);
        }
        
        //set config with right version
        for (String key : multiVersionConfig.keySet()) {
        	String val = multiVersionConfig.get(key).getLeft();
            conf.set(key, val);
            System.out.println(key + "\\t" + val);
        }
	}
	
	/**
	 * @param key
	 * @return
	 */
	private boolean shouldInclude(String key) {
		boolean include = false;
		for (String group : groups) {
			if (key.startsWith(group)) {
				include = true;
				break;
			}
		}
		
		return include;
	}
	
	/**
	 * @param fs
	 * @return
	 * @throws IOException
	 */
	private List<String> getConfigLines(InputStream inStr) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(inStr));
		String line = null; 
		List<String> lines = new ArrayList<String>();
		while((line = reader.readLine()) != null) {
			lines.add(line);
		}
		
		return lines;
	}

}
