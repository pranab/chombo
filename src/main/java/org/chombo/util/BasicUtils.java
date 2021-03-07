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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.chombo.distance.AttributeDistanceSchema;
import org.codehaus.jackson.map.ObjectMapper;

import Jama.Matrix;

/**
 * Generic Utility
 * @author pranab
 *
 */
public class BasicUtils {
	public static final Integer ZERO = 0;
	public  static final Integer ONE = 1;
	
	public static final String DEF_FIELD_DELIM = ",";
	public static final String DEF_SUB_FIELD_DELIM = ":";
	
	public static final String configDelim = ",";
	public static final String configSubFieldDelim = ":";

	public static final long MILISEC_PER_SEC = 1000;
	public static final long MILISEC_PER_MIN = 60L * MILISEC_PER_SEC;
	public static final long MILISEC_PER_HOUR = 60L * MILISEC_PER_MIN;
	public static final long MILISEC_PER_HALF_DAY = 12 * MILISEC_PER_HOUR;
	public static final long MILISEC_PER_DAY = 24 * MILISEC_PER_HOUR;
	public static final long MILISEC_PER_WEEK = 7 * MILISEC_PER_DAY;
	public static final long MILISEC_PER_MONTH = 730 * MILISEC_PER_HOUR;
	public static final long MILISEC_PER_QUARTER = 2190 * MILISEC_PER_HOUR;
	public static final long MILISEC_PER_YEAR = 365 * MILISEC_PER_DAY;

	public static final long SEC_PER_MIN = 60L ;
	public static final long SEC_PER_HOUR = 60L * SEC_PER_MIN;
	public static final long SEC_PER_HALF_DAY = 12 * SEC_PER_HOUR;
	public static final long SEC_PER_DAY = 24 * SEC_PER_HOUR;
	public static final long SEC_PER_WEEK = 7 * SEC_PER_DAY;
	public static final long SEC_PER_MONTH = 730 * SEC_PER_HOUR;
	public static final long SEC_PER_QUARTER = 2190 * SEC_PER_HOUR;
	public static final long SEC_PER_YEAR = 365 * SEC_PER_DAY;

	public static final long MIN_PER_HOUR = 60L;
	public static final long MIN_PER_DAY = 24 * MIN_PER_HOUR;
	
	public static final long HOURS_PER_DAY = 24L;

	public static final String TIME_UNIT_MS = "ms";
    public static final String TIME_UNIT_SEC = "sec";
    public static final String TIME_UNIT_MIN = "min";
    public static final String TIME_UNIT_HOUR = "hour";
    public static final String TIME_UNIT_DAY = "day";
    public static final String TIME_UNIT_WEEK = "week";
    public static final String TIME_UNIT_MONTH = "month";
    public static final String TIME_UNIT_QUARTER = "quarter";
    public static final String TIME_UNIT_SEMI_ANNUAL = "semiAnnual";
    public static final String TIME_UNIT_YEAR = "year";
    
    private static String[] timeUnits = {TIME_UNIT_MS, TIME_UNIT_SEC, TIME_UNIT_MIN, TIME_UNIT_HOUR, TIME_UNIT_DAY};
    private static long[][] timeUnitConversionTable = {
    	{1, -1, -1, -1, -1},
    	{MILISEC_PER_SEC, 1, -1, -1, -1},
    	{MILISEC_PER_MIN, SEC_PER_MIN, 1, -1, -1},
    	{MILISEC_PER_HOUR, SEC_PER_HOUR, MIN_PER_HOUR, 1, -1},
       	{MILISEC_PER_DAY, SEC_PER_DAY, MIN_PER_DAY, HOURS_PER_DAY, 1}
    };
    
    public static final String EPOCH_TIME = "epochTime";
    public static final String EPOCH_TIME_SEC = "epochTimeSec";
   
	public final static double AVERAGE_RADIUS_OF_EARTH = 6371;
    
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
     * @param array
     * @return
     */
    public static <T> List<T> toList(T[] array) {
    	List<T> list = new ArrayList<T>();
    	for (T val : array) {
    		list.add(val);
    	}
    	return list;
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
    public static String[] stringArrayFromString(String record, String delimRegex ) {
    	String[] data = null;
    	if (null != record) {
	    	data = record.split(delimRegex);
    	}
    	return data;
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
	 * @param record
	 * @param delimRegex
	 * @param subFieldDelim
	 * @return
	 */
	public static Map<String, Object> stringObjectMapFromString(String record, String delimRegex, 
			String subFieldDelim) {
		String[] items = record.split(delimRegex);
		return stringObjectMapFromString(items, subFieldDelim);
	}
	
	/**
	 * @param items
	 * @param subFieldDelim
	 * @return
	 */
	public static Map<String, Object> stringObjectMapFromString(String[] items, String subFieldDelim) {
		Map<String, Object> data = new HashMap<String, Object>();
		for (String item :  items) {
			String[] parts  = item.split(subFieldDelim);
			String key = parts[0];
			Object val = asTypedObject(parts[1]);
			data.put(key, val);
		}
    	return data;
	}

	/**
	 * @param stParamValue
	 * @param delimRegex
	 * @param subFieldDelim
	 * @param msg
	 * @param rangeInKey
	 * @return
	 */
	public static Map<Integer, Integer> integerIntegerMapFromString(String stParamValue, String delimRegex, 
			String subFieldDelim, boolean rangeInKey) {
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
	 * @param stParamValue
	 * @param delimRegex
	 * @param subFieldDelim
	 * @param dateFormatStr
	 * @param timeZone
	 * @param epochTimeMs
	 * @return
	 */
	public static Map<Long, Object> epochTimeObjectMapFromString(String stParamValue, String delimRegex, String subFieldDelim, 
			String dateFormatStr, String timeZone, boolean epochTimeMs) {
		String[] items = stParamValue.split(delimRegex);
		return epochTimeObjectMapFromString(items, subFieldDelim, dateFormatStr, timeZone,  epochTimeMs);
	}	
	
	/**
	 * @param items
	 * @param subFieldDelim
	 * @param dateFormatStr
	 * @param timeZone
	 * @param epochTimeMs
	 * @return
	 */
	public static Map<Long, Object> epochTimeObjectMapFromString(String[] items, String subFieldDelim, 
			String dateFormatStr, String timeZone, boolean epochTimeMs) {
		Map<Long, Object> data = new HashMap<Long, Object>();
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatStr);
			if (null != timeZone) {
				dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
			}
			for (String item :  items) {
				String[] parts  = item.split(subFieldDelim);
				Date date = dateFormat.parse(parts[0]);
				long epochTime = date.getTime();
				if (!epochTimeMs) {
					epochTime /= 1000;
				}
				Object val = asTypedObject(parts[1]);
				data.put(epochTime, val);
			}		
		} catch (ParseException ex) {
			throw new IllegalArgumentException("failed to parse date " + ex.getMessage());		
		}
		return data;
	}
	
	/**
	 * @param items
	 * @param subFieldDelim
	 * @param dateFormatStr
	 * @param timeZone
	 * @param epochTimeMs
	 * @return
	 */
	public static Map<Long, Integer> epochTimeIntegerMapFromString(String[] items, String subFieldDelim, 
			String dateFormatStr, String timeZone, boolean epochTimeMs) {
		Map<Long, Integer> data = new HashMap<Long, Integer>();
		Map<Long, Object> objData = epochTimeObjectMapFromString(items, subFieldDelim, dateFormatStr, timeZone, epochTimeMs);
		for (Long key : objData.keySet()) {
			Object val = objData.get(key);
			if (val instanceof Long) {
				long lVal = (Long)val;
				data.put(key, (int)lVal);
			} else {
				throw new IllegalArgumentException("expecting integer  as value");
			}
		}
		return data;
	}	
	

	/**
	 * @param value
	 * @return
	 */
	public static Object asTypedObject(String value) {
		//try integer
		Object val = asLong(value);
		
		//try floating point
		if (null == val) {
			val = asDouble(value);
		}
		
		//default string
		if (null == val) {
			val = value;
		}
		return val;
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
     * @param delim
     * @return
     */
    public static String  extractFieldsAsString(String[] items , int[] fields, String delim) {
    	String[] fieldValues = new String[fields.length];
    	for (int i = 0; i < fields.length; ++i) {
    		fieldValues[i] = items[fields[i]];
    	}
    	return join(fieldValues, delim);
    }
 
    /**
     * @param items
     * @param fields
     * @return
     */
    public static String[]  extractRemainingFieldsAsStringArray(String[] items , int[] fields) {
    	String[] fieldValues = new String[items.length - fields.length];
    	for (int i = 0, j = 0; i < items.length; ++i) {
    		if (!ArrayUtils.contains(fields, i)) {
    			fieldValues[j++] = items[i];
    		}
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
     * @return
     */
    public static int[]  toIntArray(String[] items) {
    	int[] fieldValues = new int[items.length];
    	for (int i = 0; i < items.length; ++i) {
    		fieldValues[i] = Integer.parseInt(items[i]);
    	}
    	return fieldValues;
    }

    /**
     * @param items
     * @param fields
     * @return
     */
    public static double[]  extractFieldsAsDoubleArray(String[] items , int[] fields) {
    	double[] fieldValues = new double[fields.length];
    	for (int i = 0; i < fields.length; ++i) {
    		fieldValues[i] = Double.parseDouble((items[fields[i]]));
    	}
    	return fieldValues;
    
    }
    
    /**
     * @param recItems
     * @param fields
     * @return
     */
    public static List<DoublePoint>  extractFieldsAsDoubleArrayList(String[][] recItems, int[] fields) {
    	List<DoublePoint> records = new ArrayList<DoublePoint>();
    	for (String[] items : recItems) {
    		DoublePoint pt = new DoublePoint(extractFieldsAsDoubleArray(items , fields));
    		records.add(pt);
    	}
    	return records;
    }
   
    
    /**
     * @param items
     * @return
     */
    public static double[]  toDoubleArray(String[] items) {
    	double[] fieldValues = new double[items.length];
    	for (int i = 0; i < items.length; ++i) {
    		fieldValues[i] = Double.parseDouble(items[i]);
    	}
    	return fieldValues;
    }
    
    /**
     * @param items
     * @param fields
     * @param delim
     * @return
     */
    public static String extractFields(String[] items , int[] fields, String delim) {
    	return extractFields(items , fields, delim, false);
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
     * @param index
     * @return
     */
    public static String extractStringFromStringArray(String[] items, int index) {
    	return items[index];
    }
    
    /**
     * @param items
     * @param index
     * @return
     */
    public static int extractIntFromStringArray(String[] items, int index) {
    	return Integer.parseInt(items[index]);
    }
    
    /**
     * @param items
     * @param index
     * @return
     */
    public static long extractLongFromStringArray(String[] items, int index) {
    	return Long.parseLong(items[index]);
    }

    /**
     * @param items
     * @param index
     * @return
     */
    public static double extractDoubleFromStringArray(String[] items, int index) {
    	return Double.parseDouble(items[index]);
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
     * @param valueList
     * @return
     */
    public static String[] fromListToStringArray(List<String> valueList) {
    	String[] values = new String[valueList.size()];
    	values = valueList.toArray(values);
    	return values;
    }

    /**
     * @param valueList
     * @return
     */
    public static double[] fromListToDoubleArray(List<Double> valueList) {
    	double[] values = new double[valueList.size()];
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
     * @param delim
     * @return
     */
    public static  String join(double[] arr, String delim) {
    	StringBuilder stBld = new StringBuilder();
    	for (double val : arr) {
    		stBld.append(val).append(delim);
    	}
    	
    	return stBld.substring(0, stBld.length() -1);
    }
    
    /**
     * @param arr
     * @param delim
     * @param delim
     * @return
     */
    public static  String join(double[] arr, String delim, int precision) {
    	StringBuilder stBld = new StringBuilder();
    	for (double val : arr) {
    		stBld.append(formatDouble(val, precision)).append(delim);
    	}
    	
    	return stBld.substring(0, stBld.length() -1);
    }
    
    /**
     * @param arr
     * @param delim
     * @return
     */
    public static  String join(int[] arr, String delim) {
    	StringBuilder stBld = new StringBuilder();
    	for (int val : arr) {
    		stBld.append(val).append(delim);
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
     * @param begIndex
     * @return
     */
    public static <T> String join(T[] arr, int begIndex) {
    	return join(arr,  begIndex, arr.length, ",");
    }

    /**
     * @param arr
     * @param begIndex
     * @return
     */
    public static <T> String join(T[] arr, int begIndex, String delim) {
    	return join(arr,  begIndex, arr.length, delim);
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
	public static List<Pair<Integer, Boolean>> getIntBooleanList(String record, String fieldDelim, String subFieldDelim) {
		List<Pair<Integer, Boolean>> intBooleanPairs = new ArrayList<Pair<Integer, Boolean>>();
		String[] items = record.split(fieldDelim);
		for (String item : items) {
			String[] subItems = item.split(subFieldDelim);
			Pair<Integer, Boolean> pair = new Pair<Integer, Boolean>(Integer.parseInt(subItems[0]),  new Boolean(subItems[1]));
			intBooleanPairs.add(pair);
		}
		return intBooleanPairs;
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
	 * @param list
	 * @return
	 */
	public static <T> T selectRandom(List<T> list) {
   		int index = (int)(Math.random() * list.size());
		return list.get(index);
	}
	
	/**
	 * @param list
	 * @return
	 */
	public static <T> T selectRandom(T[] arr) {
   		int index = (int)(Math.random() * arr.length);
		return arr[index];
	}
	
	/**
	 * @param arr
	 * @param size
	 * @param sampled
	 */
	public static <T> void selectRandomListWithReplacement(T[] arr, T[] sampled) {
		int size = sampled.length;
		for (int i = 0; i < size; ++i) {
			int index = (int)(Math.random() * arr.length);
			index = index < arr.length ? index : arr.length -1 ;
			sampled[i] = arr[index];
		}
	}
	

	/**
	 * @param arr
	 * @param size
	 * @param sampled
	 */
	public static <T> void selectRandomList(T[] arr, T[] sampled) {
		int size = sampled.length;
		for (int i = 0; i < size; ++i) {
			boolean exists = true;
			while(exists) {
				int index = (int)(Math.random() * arr.length);
				index = index < arr.length ? index : arr.length -1 ;
				T instance = arr[index];
				if ( i == 0) {
					exists = false;
				} else {
					for (int j = 0; j < i; ++j) {
						exists = instance.equals(sampled[j]);
						if (exists) {
							break;
						}
					}
				}
				if (!exists) {
					sampled[i] = instance;
				}
			}
		}
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
		String[] items = record.split(fieldDelim);
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
     * @param record
     * @param delimRegex
     * @return
     */
    public static String[] getTrimmedFields(String record, String delimRegex) {
    	String[] fields = record.split(delimRegex, -1);
    	for (int i = 0; i < fields.length; ++i) {
    		fields[i] = fields[i].trim();
    	}
    	return fields;
    }	
	
    /**
     * @param record
     * @param delimRegex
     * @param quoted
     * @return
     */
    public static String[] getTrimmedFields(String record, String delimRegex, boolean quoted) {
    	String[] fields = record.split(delimRegex, -1);
    	for (int i = 0; i < fields.length; ++i) {
    		fields[i] = fields[i].trim();
    	}
    	
    	//consolidate under quote to one field
    	if (quoted) {
        	List<String> consolidated = new ArrayList<String>();
        	List<String> quoteEnclosed = new ArrayList<String>();
        	boolean insideQuote = false;
        	for (String item : fields) {
        		int pos = item.indexOf("\"");
        		if (pos == -1) {
        			//no quote
        			if (insideQuote) {
        				//continue inside quote
        				quoteEnclosed.add(item);
        			} else {
        				//normal
        				consolidated.add(item);
        			}
        		} else {
        			//quote
        			if (insideQuote) {
        				//close off quote
        				quoteEnclosed.add(item);
        				String joined = BasicUtils.join(quoteEnclosed, delimRegex);
        				consolidated.add(joined);
        				quoteEnclosed.clear();
        				insideQuote = false;
        			} else {
        				//start quote
        				quoteEnclosed.add(item);
        				insideQuote = true;
        			}
        		}
        	}
        	fields = BasicUtils.fromListToStringArray(consolidated);
    	}
    	
    	return fields;
    }	

    /**
     * @param fields
     * @return
     */
    public static String[] getTrimmedFields(String[] fields) {
    	for (int i = 0; i < fields.length; ++i) {
    		fields[i] = fields[i].trim();
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
     * @param firstList
     * @param secondList
     * @return
     */
    public static <T> boolean listIncluded(List<T> firstList, List<T> secondList) {
    	boolean included = true;
    	List<T> superList = null;
    	List<T> subList = null;
    	if (firstList.size() >= secondList.size()) {
    		superList = firstList;
    		subList = secondList;
    	} else {
    		superList = secondList;
    		subList = firstList;
    	}
    	
    	for (T item : subList) {
    		if (!superList.contains(item)) {
    			included = false;
    			break;
    		}
    	}
    	return included;
    }

    /**
     * @param list
     * @param maxSubListSize
     * @return
     */
    public static <T> List<List<T>>  generateSublists(List<T> list, int maxSubListSize) {
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
     * @param values
     * @return
     */
    public static <T> List<T> generateUniqueList(List<T> values) {
    	List<T> list = new ArrayList<T>();
    	for (T item : values) {
    		if (!list.contains(item)) {
    			list.add(item);
    		}
    	}
    	return list;
    }
    
    /**
     * @param values
     * @return
     */
    public static <T> List<T> generateUniqueList(T[] values) {
    	List<T> list = new ArrayList<T>();
    	for (T item : values) {
    		if (!list.contains(item)) {
    			list.add(item);
    		}
    	}
    	return list;
    }
    
    /**
     * @param list
     * @param value
     */
    public static <T> void addToUniqueList(List<T> list, T value) {
		if (!list.contains(value)) {
			list.add(value);
		}
    }
    
    /**
     * @param list
     * @param numIter
     * @param maxSwapOffset
     */
    public static <T> void scramble(List<T> list, int numIter, int maxSwapOffset) {
    	for (int i = 0; i < numIter; ) {
    		int firstIndex = sampleUniform(list.size());
    		int secondIndex = firstIndex + sampleUniform(1, maxSwapOffset);
    		if (secondIndex < list.size()) {
    			//swap
    			T temp = list.get(secondIndex);
    			list.add(secondIndex, list.get(firstIndex));
    			list.add(firstIndex, temp);
    			++i;
    		}
    	}
    }
    
    /**
     * @param values
     * @param value
     * @return
     */
    public static Pair<Integer, Integer> getBoundingValues(List<Integer> values, int value) {
    	Pair<Integer, Integer> bounds = null;
    	for(int i = 0; i < values.size() - 1; ++i) {
    		if (value >= values.get(i) && value < values.get(i + 1)) {
    			bounds = new Pair<Integer, Integer>(values.get(i), values.get(i + 1));
    			break;
    		}
    	}
    	return bounds;
    }
   
    
    /**
     * @param values
     * @param value
     * @return
     */
    public static Pair<Double, Double> getBoundingValues(List<Double> values, double value) {
    	Pair<Double, Double> bounds = null;
    	for(int i = 0; i < values.size() - 1; ++i) {
    		if (value >= values.get(i) && value < values.get(i + 1)) {
    			bounds = new Pair<Double, Double>(values.get(i), values.get(i + 1));
    			break;
    		}
    	}
    	return bounds;
    }

    /**
     * @param val
     * @param prec
     * @return
     */
    public static String formatDouble(double val) {
    	return formatDouble(val, 3);
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
     * @param formatter
     * @return
     */
    public static String formatDouble(double val, String formatter) {
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
    public static long getEpochTime(String dateTimeStamp, SimpleDateFormat dateFormat, int timeZoneShiftHour) throws ParseException {
    	long epochTime = (null != dateFormat) ? getEpochTime(dateTimeStamp, false, dateFormat,timeZoneShiftHour) :
    		getEpochTime(dateTimeStamp, true, dateFormat,0);
    	return epochTime;
    }
    

    /**
     * @param dateTimeStamp
     * @param dateFormat
     * @return
     * @throws ParseException
     */
    public static long getEpochTime(String dateTimeStamp, SimpleDateFormat dateFormat) throws ParseException {
    	return getEpochTime(dateTimeStamp, dateFormat, 0);
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
    		if (0 != timeZoneShiftHour) {
    			epochTime += timeZoneShiftHour * MILISEC_PER_HOUR;
    		}
    	}
    	
    	return epochTime;
    }

    /**
     * @param epochTime
     * @param timeUnit
     * @return
     */
    public static long convertTimeUnit(long epochTime, String timeUnit) {
    	long modTime = epochTime;
		if (timeUnit.equals(TIME_UNIT_SEC)) {
			modTime = divideWithRoundOff(epochTime, MILISEC_PER_SEC);
		} else if (timeUnit.equals(TIME_UNIT_MIN)) {
			modTime = divideWithRoundOff(epochTime, MILISEC_PER_MIN);
		} else if (timeUnit.equals(TIME_UNIT_HOUR)) {
			modTime = divideWithRoundOff(epochTime, MILISEC_PER_HOUR);
		} else if (timeUnit.equals(TIME_UNIT_DAY)) {
			modTime = divideWithRoundOff(epochTime, MILISEC_PER_DAY);
		} else if (timeUnit.equals(TIME_UNIT_WEEK)) {
			modTime = divideWithRoundOff(epochTime, MILISEC_PER_WEEK);
		} else if (timeUnit.equals(TIME_UNIT_MONTH)) {
			modTime = divideWithRoundOff(epochTime, MILISEC_PER_MONTH);
		} else {
			throw new IllegalArgumentException("invalid time unit");
		}
    	return modTime;
    }
    
    
    /**
     * @param timeUnit
     * @return
     */
    public static long toEpochTime(String timeUnit) {
    	long epochTime = 0;
		if (timeUnit.equals(TIME_UNIT_SEC)) {
			epochTime = MILISEC_PER_SEC;
		} else if (timeUnit.equals(TIME_UNIT_MIN)) {
			epochTime = MILISEC_PER_MIN;
		} else if (timeUnit.equals(TIME_UNIT_HOUR)) {
			epochTime = MILISEC_PER_HOUR;
		} else if (timeUnit.equals(TIME_UNIT_DAY)) {
			epochTime = MILISEC_PER_DAY;
		} else if (timeUnit.equals(BasicUtils.TIME_UNIT_MONTH)) {
			epochTime = MILISEC_PER_MONTH;
		} else if (timeUnit.equals(BasicUtils.TIME_UNIT_QUARTER)) {
			epochTime = MILISEC_PER_QUARTER;
		} else {
			throw new IllegalArgumentException("invalid time unit");
		}
    	
		return epochTime;
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
     * @param value
     * @return
     */
    public  static int factorial(int value) {
    	int fact = 1;
    	for (int i = value ; i > 1; --i) {
    		fact *= i;
    	}
    	return fact;
    }
   
    /**
     * @param val
     * @return
     */
    public static boolean isInt(String val) {
    	boolean valid = true;
		try {
			int iVal = Integer.parseInt(val);
		} catch (Exception ex) {
			valid = false;
		}
    	return valid;
    }
    
    /**
     * @param val
     * @return
     */
    public static Integer asInt(String val) {
    	Integer iVal = null;
		try {
			iVal = Integer.parseInt(val);
		} catch (Exception ex) {
		}
    	return iVal;
    }

    /**
     * @param val
     * @return
     */
    public static boolean isLong(String val) {
    	boolean valid = true;
		try {
			long lVal = Long.parseLong(val);
		} catch (Exception ex) {
			valid = false;
		}
    	return valid;
    }

    /**
     * @param val
     * @return
     */
    public static Long asLong(String val) {
    	Long lVal = null;
		try {
			lVal = Long.parseLong(val);
		} catch (Exception ex) {
		}
    	return lVal;
    }
    /**
     * @param val
     * @return
     */
    public static boolean isFloat(String val) {
    	boolean valid = true;
		try {
			float dVal = Float.parseFloat(val);
		} catch (Exception ex) {
			valid = false;
		}
    	return valid;
    }
    

    /**
     * @param val
     * @return
     */
    public static boolean isDouble(String val) {
    	boolean valid = true;
		try {
			double dVal = Double.parseDouble(val);
		} catch (Exception ex) {
			valid = false;
		}
    	return valid;
    }
    
    /**
     * @param val
     * @return
     */
    public static Double asDouble(String val) {
    	Double dVal = null;
		try {
			dVal = Double.parseDouble(val);
		} catch (Exception ex) {
		}
    	return dVal;
    }

    /**
     * @param val
     * @return
     */
    public static boolean isComposite(String val, String subFieldDelim) {
    	boolean valid = true;
		String[] subItems = val.split(subFieldDelim); 
		valid = subItems.length > 1;
    	return valid;
    }    
    
    /**
     * @param val
     * @param formatter
     * @return
     */
    public static boolean isDate(String val, SimpleDateFormat formatter) {
    	boolean valid = true;
    	try {
    		Date date = formatter.parse(val);	
    		valid = null != date;
    	}catch (Exception ex) {
			valid = false;
		}
    	return valid;
    }
    
    /**
     * @param data
     * @param searchPattern
     * @return
     */
    public static int findNumOccureneces(String data, String searchPattern){
    	Pattern pattern = Pattern.compile(searchPattern);
    	return findNumOccureneces(data, pattern);
    }
    
    /**
     * @param data
     * @param pattern
     * @return
     */
    public static int findNumOccureneces(String data, Pattern pattern){
    	int count = 0;
    	Matcher matcher = pattern.matcher(data);
    	for( ;matcher.find(); ++count){}  
    	return count;
    }
    
    /**
     * @param filePath
     * @return
     * @throws IOException
     */
    public static List<String> getFileLines(String filePath) throws IOException {
    	InputStream fs = getFileStream(filePath);
    	return getFileLines(fs);
    }    
    
    /**
     * @param fs
     * @return
     * @throws IOException
     */
    public static List<String> getFileLines(InputStream fs) throws IOException {
    	List<String> lines = new ArrayList<String>();
    	if (null != fs) {
    		BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
    		String line = null; 
    		while((line = reader.readLine()) != null) {
    			//System.out.println(line);
    			lines.add(line);
    		}
    	}
    	return lines;
    }

    /**
     * @param filePath
     * @param keyLen
     * @param quantFldOrd
     * @return
     * @throws IOException
     */
    public static Map<String, Integer> getKeyedIntValues(String filePath, int keyLen, int quantFldOrd) throws IOException {
    	return getKeyedIntValues(filePath, keyLen, quantFldOrd, DEF_FIELD_DELIM);
    }  

    /**
     * @param filePath
     * @param keyLen
     * @param quantFldOrd
     * @return
     * @throws IOException
     */
    public static Map<String, Integer> getKeyedIntValues(String filePath, int keyLen, int quantFldOrd, String fieldDelim) throws IOException {
    	Map<String, String> values =  getKeyedStringValues(filePath, keyLen, quantFldOrd, fieldDelim);
    	Map<String, Integer> keyedValues = new HashMap<String, Integer>();
    	for (String key : values.keySet()) {
    		keyedValues.put(key, Integer.parseInt(values.get(key))
    				);
    	}
    	return keyedValues;
    }  

    /**
     * @param filePath
     * @param keyLen
     * @param quantFldOrd
     * @return
     * @throws IOException
     */
    public static Map<String, Long> getKeyedLongValues(String filePath, int keyLen, int quantFldOrd) throws IOException {
    	return getKeyedLongValues(filePath, keyLen, quantFldOrd, DEF_FIELD_DELIM);
    }  
   
    /**
     * @param filePath
     * @param keyLen
     * @param quantFldOrd
     * @return
     * @throws IOException
     */
    public static Map<String, Long> getKeyedLongValues(String filePath, int keyLen, int quantFldOrd, String fieldDelim) throws IOException {
    	Map<String, String> values =  getKeyedStringValues(filePath, keyLen, quantFldOrd, fieldDelim);
    	Map<String, Long> keyedValues = new HashMap<String, Long>();
    	for (String key : values.keySet()) {
    		keyedValues.put(key, Long.parseLong(values.get(key)));
    	}
    	return keyedValues;
    }  

    /**
     * @param filePath
     * @param keyLen
     * @param quantFldOrd
     * @return
     * @throws IOException
     */
    public static Map<String, Double> getKeyedValues(String filePath, int keyLen, int quantFldOrd) throws IOException {
    	return getKeyedValues(filePath, keyLen, quantFldOrd, DEF_FIELD_DELIM);
    }  
    
    /**
     * @param filePath
     * @param keyLen
     * @param quantFldOrd
     * @param fieldDelim
     * @return
     * @throws IOException
     */
    public static Map<String, Double> getKeyedValues(String filePath, int keyLen, int quantFldOrd, String fieldDelim) 
    	throws IOException {
    	Map<String, Double> keyedValues = new HashMap<String, Double>();
    	List<String> lines = getFileLines(filePath);
    	for (String line : lines) {
    		String[] items = getTrimmedFields(line, fieldDelim);
    		int pos = findOccurencePosition(line, DEF_FIELD_DELIM, keyLen, true);
    		String key = line.substring(0, pos);
    		Double value = Double.parseDouble(items[quantFldOrd]);
    		keyedValues.put(key, value);
    		//System.out.println("key: " + key + "value: " + value);
    	}
    	return keyedValues;
    }
    
    /**
     * @param filePath
     * @param keyLen
     * @param quantFldOrd
     * @return
     * @throws IOException
     */
    public static Map<String, String> getKeyedStringValues(String filePath, int keyLen, int quantFldOrd) throws IOException {
    	return getKeyedStringValues(filePath, keyLen, quantFldOrd, DEF_FIELD_DELIM);
    }  
 
    
    /**
     * @param filePath
     * @param keyLen
     * @param quantFldOrd
     * @param fieldDelim
     * @return
     * @throws IOException
     */
    public static Map<String, String> getKeyedStringValues(String filePath, int keyLen, int quantFldOrd, String fieldDelim) 
    	throws IOException {
    	Map<String, String> keyedValues = new HashMap<String, String>();
    	List<String> lines = getFileLines(filePath);
    	for (String line : lines) {
    		String[] items = getTrimmedFields(line, fieldDelim);
    		int pos = findOccurencePosition(line, DEF_FIELD_DELIM, keyLen, true);
    		String key = line.substring(0, pos);
    		String value = items[quantFldOrd];
    		keyedValues.put(key, value);
    		//System.out.println("key: " + key + "value: " + value);
    	}
    	return keyedValues;
    }

    /**
     * @param filePath
     * @param keyLen
     * @param firstQuantFldOrd
     * @param secondQuantFldOrd
     * @return
     * @throws IOException
     */
    public static Map<String, Pair<Double, Double>> getKeyedValuePairs(String filePath, int keyLen, 
    		int firstQuantFldOrd, int secondQuantFldOrd) throws IOException {
    	return getKeyedValuePairs(filePath, keyLen, firstQuantFldOrd, secondQuantFldOrd, DEF_FIELD_DELIM);
    }
   
    /**
     * @param filePath
     * @param keyLen
     * @param firstQuantFldOrd
     * @param secondQuantFldOrd
     * @param fieldDelim
     * @return
     * @throws IOException
     */
    public static Map<String, Pair<Double, Double>> getKeyedValuePairs(String filePath, int keyLen, 
		int firstQuantFldOrd, int secondQuantFldOrd, String fieldDelim) 
    	throws IOException {
    	Map<String, Pair<Double, Double>> keyedValues = new HashMap<String, Pair<Double, Double>>();
    	List<String> lines = getFileLines(filePath);
    	for (String line : lines) {
    		String[] items = line.split(DEF_FIELD_DELIM, -1);
    		int pos = findOccurencePosition(line, DEF_FIELD_DELIM, keyLen, true);
    		String key = line.substring(0, pos);
    		Double fValue = Double.parseDouble(items[firstQuantFldOrd]);
    		Double sValue = Double.parseDouble(items[secondQuantFldOrd]);
    		keyedValues.put(key, new Pair<Double, Double>(fValue, sValue));
    	}
    	return keyedValues;
      }

    /**
     * @param values
     * @param value
     * @return
     */
    public static boolean contains(int[] values, int value) {
    	boolean doesContain = false;
    	for (int thisValue : values) {
    		if (thisValue == value) {
    			doesContain = true;
    			break;
    		}
    	}
    	return doesContain;
    }
   
    /**
     * @param values
     * @param value
     * @return
     */
    public static boolean contains(String[] values, String value) {
    	boolean doesContain = false;
    	for (String thisValue : values) {
    		if (thisValue.equals(value)) {
    			doesContain = true;
    			break;
    		}
    	}
    	return doesContain;
    }

    /**
     * @param arr
     * @return
     */
    public static <T> Set<T> generateSetFromArray(T[] arr) {
    	Set<T> set = new HashSet<T>();
    	for (T obj : arr) {
    		set.add(obj);
    	}
    	return set;
    }
    
    /**
     * @param arr
     * @return
     */
    public static <T> Set<T> generateSetFromList(List<T> list) {
    	Set<T> set = new HashSet<T>();
    	for (T obj : list) {
    		set.add(obj);
    	}
    	return set;
    }
    
    /**
     * @param arr
     * @return
     */
    public static <T> Set<T> generateSetFromList(T[] array) {
    	Set<T> set = new HashSet<T>();
    	for (T obj : array) {
    		set.add(obj);
    	}
    	return set;
    }

    /**
     * @param conf
     * @param pathConfig
     * @return
     * @throws IOException
     */
    public static InputStream getFileStream(String filePath) throws IOException {
    	InputStream fiStrm = null;
        if (null != filePath) {
        	fiStrm = new FileInputStream(filePath);
        }
        return fiStrm;
    }
    
	/**
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static RichAttributeSchema getRichAttributeSchema(String path) throws IOException {
        InputStream fs = new FileInputStream(path);
        ObjectMapper mapper = new ObjectMapper();
        RichAttributeSchema schema = mapper.readValue(fs, RichAttributeSchema.class);
        return schema;
	}
   
	/**
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static RichAttributeSchema getRichAttributeSchema(InputStream fs) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        RichAttributeSchema schema = mapper.readValue(fs, RichAttributeSchema.class);
        return schema;
	}

	/**
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static GenericAttributeSchema getGenericAttributeSchema(String path) throws IOException {
        InputStream fs = new FileInputStream(path);
        ObjectMapper mapper = new ObjectMapper();
        GenericAttributeSchema schema = mapper.readValue(fs, GenericAttributeSchema.class);
        return schema;
	}
	
	/**
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static GenericAttributeSchema getGenericAttributeSchema(InputStream fs) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        GenericAttributeSchema schema = mapper.readValue(fs, GenericAttributeSchema.class);
        return schema;
	}

	/**
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static FeatureSchema getFeatureSchema(String path) throws IOException {
        InputStream fs = new FileInputStream(path);
        ObjectMapper mapper = new ObjectMapper();
        FeatureSchema schema = mapper.readValue(fs, FeatureSchema.class);
        return schema;
	}
	
	/**
	 * @param fs
	 * @return
	 * @throws IOException
	 */
	public static FeatureSchema getFeatureSchema(InputStream fs) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        FeatureSchema schema = mapper.readValue(fs, FeatureSchema.class);
        return schema;
	}

	/**
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static ProcessorAttributeSchema getProcessingSchema(String path) throws IOException {
        InputStream fs = new FileInputStream(path);
        ObjectMapper mapper = new ObjectMapper();
        ProcessorAttributeSchema schema = mapper.readValue(fs, ProcessorAttributeSchema.class);
        return schema;
	}
	
	/**
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static ProcessorAttributeSchema getProcessingSchema(InputStream fs) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ProcessorAttributeSchema schema = mapper.readValue(fs, ProcessorAttributeSchema.class);
        return schema;
	}

	/**
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static AttributeDistanceSchema getDistanceSchema(String path) throws IOException {
        InputStream fs = new FileInputStream(path);
        ObjectMapper mapper = new ObjectMapper();
        AttributeDistanceSchema schema = mapper.readValue(fs, AttributeDistanceSchema.class);
        return schema;
	}

	/**
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static AttributeDistanceSchema getDistanceSchema(InputStream fs) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        AttributeDistanceSchema schema = mapper.readValue(fs, AttributeDistanceSchema.class);
        return schema;
	}

	/**
	 * @param value
	 * @param delim
	 * @return
	 */
	public static String[] splitOnFirstOccurence(String value, String delim, boolean failOnDelimNotFound) {
		int pos = value.indexOf(delim);
		return splitOnPosition(value, pos, delim.length(),failOnDelimNotFound);
	}
	
	/**
	 * @param value
	 * @param delim
	 * @return
	 */
	public static String[] splitOnLastOccurence(String value, String delim, boolean failOnDelimNotFound) {
		int pos = value.lastIndexOf(delim);
		return splitOnPosition(value, pos, delim.length(), failOnDelimNotFound);
	}
	
	/**
	 * @param value
	 * @param pos
	 * @return
	 */
	public static String[] splitOnPosition(String value, int pos, boolean failOnDelimNotFound) {
		String[] items = splitOnPosition(value, pos, 1,  failOnDelimNotFound) ;
		return items;
	}

	/**
	 * @param value
	 * @param pos
	 * @return
	 */
	public static String[] splitOnPosition(String value, int pos, int delimLen,  boolean failOnDelimNotFound) {
		String[] items = new String[2];
		if (pos >= 0 && pos < value.length()) {
			items[0] = value.substring(0, pos);
			items[1] = value.substring(pos + delimLen);
		} else {
			if (failOnDelimNotFound) {
				throw new IllegalArgumentException("delimiter not found");
			} else {
				items[0] = value;
				items[1] = "";
			}
		}
		return items;
	}

	/**
	 * @param value
	 * @param fieldDelim
	 * @param delimPos
	 * @param failOnDelimNotFound
	 * @return
	 */
	public static String splitStringOnDelimPosition(String value, String fieldDelim, int delimPos, boolean failOnDelimNotFound) {
		int pos = findOccurencePosition(value, fieldDelim, delimPos, true);
		return  value.substring(0, pos);
	}
	
	/**
	 * @param value
	 * @param delim
	 * @param numOccurence
	 * @param failOnDelimNotFound
	 * @return
	 */
	public static int findOccurencePosition(String value, String delim, int numOccurence) {
		return findOccurencePosition(value, delim, numOccurence, false);
	}
	
	/**
	 * @param value
	 * @param delim
	 * @param numOccurence
	 * @param failOnDelimNotFound
	 * @return
	 */
	public static int findOccurencePosition(String value, String delim, int numOccurence, boolean failOnDelimNotFound) {
		int delimLen = delim.length();
		int pos = -1;
		int from = 0;
		//System.out.println(value);
		//System.out.println(delim + "  " + numOccurence);
		for (int i = 0;  i < numOccurence; ++i ) {
			pos = value.indexOf(delim, from);
			if (pos == -1) {
				if (failOnDelimNotFound) {
					throw new IllegalStateException("less than required number of occurences");
				} else {
					break;
				}
			} else {
				from = pos + delimLen;
			}
		}
		return pos;
	}
	
	/**
	 * @param value
	 * @param delim
	 * @param numOccurence
	 * @return
	 */
	public static int[] findAllOccurencePositions(String value, String delim, int numOccurence) {
		return findAllOccurencePositions(value, delim, numOccurence, false);
	}	
	
	/**
	 * @param value
	 * @param delim
	 * @param numOccurence
	 * @param failOnDelimNotFound
	 * @return
	 */
	public static int[] findAllOccurencePositions(String value, String delim, int numOccurence, boolean failOnDelimNotFound) {
		int delimLen = delim.length();
		int pos = -1;
		int from = 0;
		int[] positions = new int[numOccurence];
		for (int i = 0; i < numOccurence; ++i){
			positions[i] = -1;
		}
		
		for (int i = 0;  i < numOccurence; ++i ) {
			pos = value.indexOf(delim, from);
			if (pos == -1) {
				if (failOnDelimNotFound) {
					throw new IllegalStateException("less than required number of occurences");
				} else {
					break;
				}
			} else {
				positions[i] = pos;
				from += pos + delimLen;
			}
		}
		return positions;
	}
	
	/**
	 * @param value
	 * @param offset
	 * @return
	 */
	public static String slice(String value, int offset) {
		return slice( value, offset, 0);
	}	
	
	/**
	 * @param value
	 * @param offsetBeg
	 * @param offsetEnd
	 * @return
	 */
	public static String slice(String value, int offsetBeg, int offsetEnd) {
		String sliced = null;
		if (offsetBeg + offsetEnd < value.length()) {
			sliced = value.substring(offsetBeg, value.length() - offsetEnd);
		} else {
			throw  new IllegalStateException("invalid begi and end offsets for slicing");
		}
		return sliced;
	}

	/**
	 * @param value
	 * @param offset
	 * @return
	 */
	public static String rightSlice(String value, int offset) {
		return value.substring(offset);
	}
	
	/**
	 * @param value
	 * @param offset
	 * @return
	 */
	public static String leftSlice(String value, int offset) {
		return value.substring(0, value.length() - offset);
	}

	/**
     * geo location distance by Haversine formula
     * @param lat1
     * @param long1
     * @param lat2
     * @param long2
     * @return distance in km
     */
    public static double getGeoDistance(double lat1, double long1, double lat2, double long2) {
        double latDistance = Math.toRadians(lat1 - lat2);
        double longDistance = Math.toRadians(long1 - long2);

        double a = (Math.sin(latDistance / 2) * Math.sin(latDistance / 2)) + (Math.cos(Math.toRadians(lat1))) *
                        (Math.cos(Math.toRadians(lat2))) * (Math.sin(longDistance / 2)) * (Math.sin(longDistance / 2));
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return AVERAGE_RADIUS_OF_EARTH * c;

    }    	
    
    /**
     * @param obj
     * @return
     */
    public static <T> int positiveHashCode(T obj) {
    	int hashCode = obj.hashCode();
    	if (hashCode < 0)
    		hashCode = -hashCode;
    	return hashCode;
    }
    
	/**
	 * @param val1st
	 * @param val2nd
	 * @return
	 */
    public static  double max(double val1st, double val2nd) {
		return val1st > val2nd ? val1st : val2nd;
	}

	/**
	 * @param val1st
	 * @param val2nd
	 * @return
	 */
	public static  double min(double val1st, double val2nd) {
		return val1st < val2nd ? val1st : val2nd;
	}

	/**
	 * @param val
	 * @param min
	 * @param max
	 * @return
	 */
	public static double between(double val, double min, double max) {
		return val < min ? min : (val > max ? max : val);
	}

	/**
	 * @param val1st
	 * @param val2nd
	 * @return
	 */
	public static int max(int val1st, int val2nd) {
		return val1st > val2nd ? val1st : val2nd;
	}

	/**
	 * @param val1st
	 * @param val2nd
	 * @return
	 */
	public static int min(int val1st, int val2nd) {
		return val1st < val2nd ? val1st : val2nd;
	}
	
	/**
	 * @param val
	 * @param min
	 * @param max
	 * @return
	 */
	public static int between(int val, int min, int max) {
		return val < min ? min : (val > max ? max : val);
	}
	
	/**
	 * @param max
	 * @return
	 */
	public static int sampleUniform(int max) {
		return sampleUniform(0,  max);
	}
    
	/**
	 * @param max
	 * @param excludes
	 * @return
	 */
	public static int sampleUniform(int max,Set<Integer> excludes) {
		return sampleUniform(0, max, excludes);
	}

	/**
	 * @param min inclusive
	 * @param max inclusive
	 * @return
	 */
	public static int sampleUniform(int min, int max) {
		long val = min + Math.round(Math.random() * (max - min));
		return (int)val;
	}
	
	/**
	 * @param min
	 * @param max
	 * @param excludes
	 * @return
	 */
	public static int sampleUniform(int min, int max, Set<Integer> excludes) {
		long val = min + Math.round(Math.random() * (max - min));
		while (excludes.contains(val)) {
			val = min + Math.round(Math.random() * (max - min));
		}
		return (int)val;
	}
	
	/**
	 * @param min
	 * @param max
	 * @return
	 */
	public static float sampleUniform(float min, float max) {
		float val = min + (float)(Math.random() * (max - min));
		return val;
	}
	
	/**
	 * @param min
	 * @param max
	 * @return
	 */
	public static double sampleUniform(double min, double max) {
		double val = min + Math.random() * (max - min);
		return val;
	}
	
	/**
	 * Excludes max value
	 * @param min
	 * @param max
	 * @return
	 */
	public static int sampleUniformExclude(int min, int max) {
		long val = min +( long)(Math.random() * (max - min));
		return (int)val;
	}

	/**
	 * Excludes max value
	 * @param max
	 * @return
	 */
	public static int sampleUniformExclude( int max) {
		return sampleUniformExclude( 0, max);
	}
	
	/**
	 * @param size
	 * @param min
	 * @param max
	 * @return
	 */
	public static double[] createDoubleArrayWithRandomValues(int size, double min, double max) {
		double[] ar = new double[size];
		for (int i = 0; i < size; ++i) {
			ar[i] = sampleUniform(min, max);
		}
		return ar;
	}
	
	/**
	 * @param size
	 * @param pos
	 * @return
	 */
	public static double[] createOneHotDoubleArray(int size, int pos) {
		double[] ar = new double[size];
		for (int i = 0; i < size; ++i) {
			ar[i] = 0;
		}
		ar[pos] = 1;
		return ar;
	}
	
	/**
	 * @param size
	 * @param pos
	 * @return
	 */
	public static int[] createOneHotIntArray(int size, int pos) {
		int[] ar = new int[size];
		for (int i = 0; i < size; ++i) {
			ar[i] = 0;
		}
		ar[pos] = 1;
		return ar;
	}
	
	/**
	 * @param size
	 * @return
	 */
	public static double[] createZeroFilledDoubleArray(int size) {
		double[] ar = new double[size];
		for (int i = 0; i < size; ++i) {
			ar[i] = 0;
		}
		return ar;
	}

	/**
	 * @param size
	 * @return
	 */
	public static int[] createZeroFilledIntArray(int size) {
		int[] ar = new int[size];
		for (int i = 0; i < size; ++i) {
			ar[i] = 0;
		}
		return ar;
	}
	
	/**
	 * @param arr
	 * @param extension
	 * @return
	 */
	public static double[] extendDoubleArray(double[] arr, int extension) {
		double[] newArr = new double[arr.length + extension];
		System.arraycopy(arr, 0, newArr, 0, arr.length);
		return newArr;
	}

	/**
	 * @param arr
	 * @param extension
	 * @return
	 */
	public static int[] extendIntArray(double[] arr, int extension) {
		int[] newArr = new int[arr.length + extension];
		System.arraycopy(arr, 0, newArr, 0, arr.length);
		return newArr;
	}

	/**
	 * @param src
	 * @param srcBeg
	 * @param srcEnd
	 * @param dest
	 * @param destbeg
	 */
	public static <T> void arrayCopy(T[] src, int srcBeg, int srcEnd, T[] dest, int destbeg) {
		//end index is exclusive
		int copySize = srcEnd - srcBeg;
		if (copySize > 0 && dest.length >= copySize + destbeg)  {
			for (int i = 0; i < copySize; ++i) {
				dest[destbeg + i] = src[srcBeg + i];
			}
		} else {
			throw new IllegalStateException("destination array too small or invalid array index");
		}
	}
	
	/**
	 * @param src
	 * @param srcBeg
	 * @param srcEnd
	 * @param dest
	 */
	public static <T> void arrayCopy(T[] src, int srcBeg, int srcEnd, T[] dest) {
		arrayCopy(src, srcBeg, srcEnd, dest, 0);
	}
	
	/**
	 * @param src
	 * @param dest
	 */
	public static <T> void arrayCopy(T[] src,  T[] dest) {
		arrayCopy(src, 0, src.length, dest, 0);
	}
	
	/**
	 * @param src
	 * @param srcBeg
	 * @param srcEnd
	 * @param dest
	 * @param destbeg
	 */
	public static <T> void listToArrayCopy(List<T> src, int srcBeg, int srcEnd, T[] dest, int destbeg) {
		//end index is exclusive
		int copySize = srcEnd - srcBeg;
		if (copySize > 0 && dest.length >= copySize + destbeg)  {
			for (int i = 0; i < copySize; ++i) {
				dest[destbeg + i] = src.get(srcBeg + i);
			}
		} else {
			throw new IllegalStateException("destination array too small or invalid array index");
		}
	}
	
	/**
	 * @param src
	 * @param srcBeg
	 * @param srcEnd
	 * @param dest
	 */
	public static <T> void listToArrayCopy(List<T> src, int srcBeg, int srcEnd, T[] dest) {
		listToArrayCopy(src, srcBeg, srcEnd, dest, 0);
	}
	
	/**
	 * @param src
	 * @param srcBeg
	 * @param srcEnd
	 * @param dest
	 * @param destbeg
	 */
	public static <T> void listToArrayCopy(List<T> src,  T[] dest, int destbeg) {
		listToArrayCopy(src, 0, src.size(), dest, destbeg);
	}	
	
	/**
	 * @param items
	 * @return
	 */
	public static int missingFieldCount(String[] items, int beg) {
    	int count = 0;
    	for (int i = beg ; i < items.length; ++i) {
    		if (items[i].isEmpty()) {
    			++count;
    		}
    	} 
		return count;
	}
	
	/**
	 * @param items
	 * @return
	 */
	public static int missingFieldCount(String[] items) {
		return missingFieldCount(items, 0);
	}
	
	/**
	 * @param items
	 * @return
	 */
	public static boolean anyMissingField(String[] items) {
		return missingFieldCount(items) > 0;
	}
	
	/**
	 * @param items
	 * @param beg
	 * @param nullTag
	 * @return
	 */
	public static int nullFieldCount(String[] items, int beg, String nullTag) {
    	int count = 0;
    	for (int i = beg ; i < items.length; ++i) {
    		if (isNull(items[i], nullTag)) {
    			++count;
    		}
    	} 
		return count;
	}
	
	/**
	 * @param items
	 * @param nullTag
	 * @return
	 */
	public static int nullFieldCount(String[] items, String nullTag) {
		return nullFieldCount(items, 0, nullTag);
	}
	
	/**
	 * @param value
	 * @param nullTag
	 * @return
	 */
	public static boolean isNull(String value, String nullTag) {
		return value.isEmpty() || value.equals(nullTag);
	}
	
	/**
	 * @param value
	 * @param strength
	 * @return
	 */
	public static String scramble(String value, int strength) {
		String scrambled = value;
		int len = value.length();
		strength = strength < 4 ? 4 : strength;
		for (int i = 0; i < strength; ++i) {
			int beg =sampleUniformExclude(len);
			int end =sampleUniformExclude(len);
			while (end == beg) {
				end =sampleUniformExclude(len);
			}
			
			if (end < beg) {
				int temp = beg;
				beg = end;
				end = temp;
			}
			
			scrambled = scrambled.substring(0,beg) + scrambled.substring(end) + scrambled.substring(beg, end);
		}
		return scrambled;
	}
	
    /**
     * @param value
     * @param divisor
     * @return
     */
    public static long divideWithRoundOff(long value, long divisor) {
    	long quotient = value / divisor;
		if ((value % divisor) > (divisor / 2)) {
			++quotient;
		}
    	return quotient;
    }
    
    /**
     * @param value
     * @return
     */
    public static long roundToLong(double value) {
    	return Math.round(value);
    }
	
    /**
     * @param value
     * @return
     */
    public static int roundToInt(double value) {
    	return (int)(Math.round(value));
    }
    
    /**
     * @param items
     * @param index
     * @return
     */
    public static int getIntField(String[] items, int index) {
    	return Integer.parseInt(items[index]);
    }
    
    /**
     * @param items
     * @param index
     * @return
     */
    public static long getLongField(String[] items, int index) {
    	return Long.parseLong(items[index]);
    }
    
    /**
     * @param items
     * @param index
     * @return
     */
    public static double getFloatField(String[] items, int index) {
    	return Float.parseFloat(items[index]);
    }

    /**
     * @param items
     * @param index
     * @return
     */
    public static double getDoubleField(String[] items, int index) {
    	return Double.parseDouble(items[index]);
    }
    
    /**
     * @param commands
     * @param execDir
     * @return
     */
    public static String execShellCommand(List<String> commands, String execDir)  {
    	String result = null;
    	try {
	    	//Run macro on target
	        ProcessBuilder pb = new ProcessBuilder(commands);
	        pb.directory(new File(execDir));
	        pb.redirectErrorStream(true);
	        Process process = pb.start();
	        
	        //read output
	        StringBuilder out = new StringBuilder();
	        BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
	        String line = null, previous = null;
	        while ((line = br.readLine()) != null) {
	            if (!line.equals(previous)) {
	                previous = line;
	                out.append(line).append('\n');
	                System.out.println(line);
	            }
	        }
	        
	        //check result
	        int exitCode = process.waitFor();
	        if (exitCode == 0) {
	        	result = out.toString();
	        }  else {
	        	throw new RuntimeException("process exited abnormally with exit code" + exitCode);
	        }
    	} catch (IOException ex) {
        	throw new RuntimeException("process execution error " + ex.getMessage());
    	} catch (InterruptedException ex) {
        	throw new RuntimeException("process execution error " + ex.getMessage());
    	}
        return result;
    }
    
    /**
     * @param data
     * @param delimRegex
     * @param guard
     * @param delim
     * @param repl
     * @return
     */
    public static String[] splitWithEmbeddedDelim(String data, String delimRegex, String guard, 
    		String delim, String repl) {
    	boolean done = false;
    	int guardLen = guard.length();
    	String value = data;
    	while(!done) {
    		int[] positions = findAllOccurencePositions(value, guard, 2);
    		if (positions[0] >= 0) {
    			if (positions[1] >= 0) {
    				int leftBeg = 0;
    				int leftEnd = positions[0];
    				int midBeg = positions[0] + guardLen;
    				int midEnd = positions[1];
    				int rightBeg = positions[1] + guardLen;
    				
    				String left = value.substring(leftBeg, leftEnd);
    				String mid = value.substring(midBeg, midEnd);
    				String right = value.substring(rightBeg);
    				mid = mid.replaceAll(delim, repl);
    				value = left + delim + mid + delim + right;
    			} else {
    				throw new IllegalStateException("no matching guard delimeter found");
    			}
    		} else {
    			done = true;
    		}
    	}
    	String[] items = value.split(delimRegex);
    	for (int i = 0; i < items.length; ++i) {
    		items[i] = items[i].replaceAll(repl, delim);
    	}
    	return items;
    }
    
    /**
     * @param cons
     * @param val
     * @return
     */
    public static double expScale(double cons, double val) {
    	double e = Math.exp(cons * val);
    	return (e - 1) / e;
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
     * @param obj
     * @param msg
     */
    public static void assertNotNull(Object obj, String msg) {
    	assertCondition(null != obj, msg);
    }
   
    /**
     * @param cond
     * @param msg
     */
    public static void assertCondition(boolean cond, String msg) {
    	if (!cond)
    		throw new IllegalStateException(msg);
    }
    
    /**
     * @param msg
     */
    public static void assertFail(String msg) {
    	throw new IllegalStateException(msg);
    }

    /**
     * @param arr
     * @param value
     * @return
     */
    public static int indexOfStringArray(String[] arr, String value) {
    	return indexOfStringArray(arr, value, true);
    }
    
    /**
     * @param arr
     * @param value
     * @param throwEx
     * @return
     */
    public static int indexOfStringArray(String[] arr, String value, boolean throwEx) {
    	int index = -1;
    	for (int i = 0; i < arr.length; ++i) {
    		if (value.equals(arr[i])) {
    			index = i;
    			break;
    		}
    	}
    	if (throwEx && -1 == index) {
    		throw new ArrayIndexOutOfBoundsException("string element " + value + " not found in array");
    	}
    	return index;
    }

    /**
     * @param arr
     * @param value
     * @return
     */
    public static int indexOfIntArray(int[] arr, int value) {
    	return indexOfIntArray(arr, value, true);
    }
    
    /**
     * @param arr
     * @param value
     * @param throwEx
     * @return
     */
    public static int indexOfIntArray(int[] arr, int value, boolean throwEx) {
    	int index = -1;
    	for (int i = 0; i < arr.length; ++i) {
    		if (value == arr[i]) {
    			index = i;
    			break;
    		}
    	}
    	if (throwEx && -1 == index) {
    		throw new ArrayIndexOutOfBoundsException("int element " + value + " not found in array");
    	}
    	return index;
    }
 
    /**
     * @param arr
     * @param value
     * @return
     */
    public static int indexOfDoubleArray(double[] arr, double value) {
    	return indexOfDoubleArray(arr, value, true);
    }    

    /**
     * @param arr
     * @param value
     * @param throwEx
     * @return
     */
    public static int indexOfDoubleArray(double[] arr, double value, boolean throwEx) {
    	int index = -1;
    	for (int i = 0; i < arr.length; ++i) {
    		if (value == arr[i]) {
    			index = i;
    			break;
    		}
    	}
    	if (throwEx && -1 == index) {
    		throw new ArrayIndexOutOfBoundsException("double element " + value + " not found in array");
    	}
    	return index;
    }
    
    /**
     * @param timeVal
     * @param fromUnit
     * @param toUnit
     * @return
     */
    public static double convertTimeUnit(double timeVal, String fromUnit, String toUnit) {
    	double converted = 0;
    	int fromIndex = indexOfStringArray(timeUnits, fromUnit);
    	int toIndex = indexOfStringArray(timeUnits, toUnit);
    	long convFactor = timeUnitConversionTable[fromIndex][toIndex];
    	if (convFactor > 0) {
    		//smaller to larger
    		converted = timeVal * convFactor;
    	} else {
    		//larger to smaller
    		convFactor = timeUnitConversionTable[toIndex][fromIndex];
    		converted = timeVal / convFactor;
    	}
    	return converted;
    }
    
    /**
     * @param arr
     * @return
     */
    public static int[] fromIntegerArraytoIntArray(Integer[] arr) {
    	int[] intArray = new int[arr.length];
    	for (int i = 0; i < arr.length; ++i) {
    		intArray[i] = arr[i];
    	}
    	return intArray;
    }
    
    /**
     * Finds all occurences of a sub sequence
     * @param source
     * @param sub
     * @return
     */
    public static int[] findStringSubSequencePositions(String[] source, String[] sub) {
    	int lastPos = source.length - sub.length;
    	List<Integer> matchedPositions = new ArrayList<Integer>();
    	for (int i = 0; i <= lastPos; ) {
    		boolean matched = true;
    		for (int j = 0; j < sub.length; ++j) {
    			if (!source[i+j].equals(sub[j])) {
    				matched = false;
    				break;
    			}
    		}
			if (matched) {
				matchedPositions.add(i);
				i += sub.length;
			} else {
				++i;
			}
    	}
    	
    	return fromListToIntArray(matchedPositions);
    }
    
    /**
     * @param value
     * @return
     */
    public static double log2(double value) {
    	return Math.log10(value) / Math.log10(2.0);
    }
    
    /**
     * @param urlStr
     * @param inputJson
     * @return
     */
    public static String httpJsonPost(String urlStr, String inputJson) {
  		StringBuilder stBld = new StringBuilder();
  		HttpURLConnection conn = null;
  		try {
  			//set up connection
	  		URL url = new URL(urlStr);
	  		conn = (HttpURLConnection) url.openConnection();
	  		conn.setDoOutput(true);
	  		conn.setRequestMethod("POST");
	  		conn.setRequestProperty("Content-Type", "application/json");
	
	  		//send request
	  		OutputStream os = conn.getOutputStream();
	  		os.write(inputJson.getBytes());
	  		os.flush();
	
	  		//HttpURLConnection.HTTP_OK
	  		if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
	  			throw new RuntimeException("Failed : HTTP error code : "+ conn.getResponseCode());
	  		}
	
	  		//process response
	  		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
	  		String output;
	  		while ((output = br.readLine()) != null) {
	  			stBld.append(output);
	  		}
  		} catch (MalformedURLException e) {
  			throw new RuntimeException("URL frmat error " + e.getMessage());
  		} catch (IOException e) {
  			throw new RuntimeException("URL frmat error " + e.getMessage());
  		} finally {
  			if (null != conn) {
  				conn.disconnect();
  			}
  		}
    	return stBld.toString();
    }
    
    /**
     * @param ref
     * @param step
     * @param numSteps
     * @param direction
     * @return
     */
    public static int[] traverse(int ref, int step, int numSteps, int direction) {
    	int[] grid = null;
    	if (direction > 0) {
    		//positive
    		grid = traversePos(ref, step, numSteps);
    	} else if (direction < 0) {
    		//negative
    		grid = traverseNeg(ref, step, numSteps);
    	} else {
    		//both
    		int[] gridNeg = traverseNeg(ref, step, numSteps);
    		int[] gridPos = traversePos(ref, step, numSteps);
    		grid = new int[2 * numSteps + 1];
    		int k = 0;
    		for (int i = 0; i < numSteps; ++i) {
    			grid[k++] = gridNeg[i];
    		}
    		for (int i = 0; i <= numSteps; ++i) {
    			grid[k++] = gridPos[i];
    		}
    	}
    	return grid;
    }
    
    /**
     * @param ref
     * @param step
     * @param numSteps
     * @return
     */
    public static int[] traversePos(int ref, int step, int numSteps) {
    	int[] grid = new int[numSteps + 1];
    	int cur = ref;
    	for (int i = 0; i <= numSteps; ++i){
    		grid[i] = cur;
    		cur += step;
    	}
    	return grid;
    }
    
    /**
     * @param ref
     * @param step
     * @param numSteps
     * @return
     */
    public static int[] traverseNeg(int ref, int step, int numSteps) {
    	int[] grid = new int[numSteps + 1];
    	int cur = ref - step * numSteps;
    	for (int i = 0; i <= numSteps; ++i){
    		grid[i] = cur;
    		cur += step;
    	}
    	return grid;
    }
    
    /**
     * @param ref
     * @param step
     * @param numSteps
     * @param direction
     * @return
     */
    public static double[] traverse(double ref, double step, int numSteps, int direction) {
    	double[] grid = null;
    	if (direction > 0) {
    		//positive
    		grid = traversePos(ref, step, numSteps);
    	} else if (direction < 0) {
    		//negative
    		grid = traverseNeg(ref, step, numSteps);
    	} else {
    		//both
    		double[] gridNeg = traverseNeg(ref, step, numSteps);
    		double[] gridPos = traversePos(ref, step, numSteps);
    		grid = new double[2 * numSteps + 1];
    		int k = 0;
    		for (int i = 0; i < numSteps; ++i) {
    			grid[k++] = gridNeg[i];
    		}
    		for (int i = 0; i <= numSteps; ++i) {
    			grid[k++] = gridPos[i];
    		}
    	}
    	return grid;
    }
   
    /**
     * @param ref
     * @param step
     * @param numSteps
     * @return
     */
    public static double[] traversePos(double ref, double step, int numSteps) {
    	double[] grid = new double[numSteps + 1];
    	double cur = ref;
    	for (int i = 0; i <= numSteps; ++i){
    		grid[i] = cur;
    		cur += step;
    	}
    	return grid;
    }
    
    /**
     * @param ref
     * @param step
     * @param numSteps
     * @return
     */
    public static double[] traverseNeg(double ref, double step, int numSteps) {
    	double[] grid = new double[numSteps + 1];
    	double cur = ref - step * numSteps;
    	for (int i = 0; i <= numSteps; ++i){
    		grid[i] = cur;
    		cur += step;
    	}
    	return grid;
    }
    
    /**
     * @param dim
     * @param value
     * @return
     */
    public static int[] initIntArray(int dim, int value) {
    	int[] arr = new int[dim];
    	for (int i = 0; i < dim; ++i) {
    		arr[i] = value;
    	}
    	return arr;
    }
    
    /**
     * @param dim
     * @param value
     * @return
     */
    public static double[] initDoubleArray(int dim, double value) {
    	double[] arr = new double[dim];
    	for (int i = 0; i < dim; ++i) {
    		arr[i] = value;
    	}
    	return arr;
    }
    
    /**
     * @param nestedList
     * @return
     */
    public static <T> List<T> flatten(List<List<T>> nestedList) {
    	List<T> list = new ArrayList<T>();
    	for( List<T> l : nestedList) {
    		list.addAll(l);
    	}
    	return list;
    }
    
    /**
     * string hash with various algorithm. Always returns positive
     * @param value
     * @return positive hash code
     */
    public static int hashCode(String value, String algo)  {
    	int hashCode = 0;
    	if (algo.equals("default")) {
    		hashCode = value.hashCode();
    		if (hashCode < 0)
    			hashCode = - hashCode;
    	} else if (algo.equals("simple")) {
    		//simple
    		long hash = 7;
    		for (byte ch : value.getBytes()) {
    		    hash = hash * 31 + ch;
    		} 
	    	if (hash < 0)
	    		hash = -hash;
    		hashCode = (int)(hash % Integer.MAX_VALUE);
    	} else if (algo.equals("FNV")) {
    		//FowlerNashVo
	    	long hash = 2166136261L;
	    	for (byte ch : value.getBytes()) {
	    		hash = hash ^ ch;
	    		hash *= 16777619;
	    	}
	    	if (hash < 0)
	    		hash = -hash;
	       	hashCode = (int)(hash % Integer.MAX_VALUE);
    	} else if (algo.equals("mumur")){
    		//murmur
    		byte[] data = value.getBytes();
    		int seed = 101;
			int m = 0x5bd1e995;
			int r = 24;

			int len = data.length;
			long h = seed ^ len;
			int lenFour = len >> 2;
		    for (int i = 0;  i < lenFour;  i++) {
		    	int iFour = i << 2;
		    	long k = data[iFour + 3];
		    	k = k << 8;
		    	k = k | (data[iFour + 2] & 0xff);
		    	k = k << 8;
		    	k = k | (data[iFour + 1] & 0xff);
		    	k = k << 8;
		    	k = k | (data[iFour + 0] & 0xff);
		    	k *= m;
		    	k ^= k >>> r;
		    	k *= m;
		    	h *= m;
		    	h ^= k;
		    }

		    int lenM = lenFour << 2;
		    int left = len - lenM;
		    if (left != 0) {
		    	if (left >= 3) {
		    			h ^= (int) data[len - 3] << 16;
		    	}
		    	if (left >= 2) {
		    			h ^= (int) data[len - 2] << 8;
		    	}
		    	if (left >= 1) {
		    			h ^= (int) data[len - 1];
		    	}
		    	h *= m;
		    }

		    h ^= h >>> 13;
		    h *= m;
		    h ^= h >>> 15;
		    if (h < 0)
		    	h = -h;
		    hashCode = (int)(h % Integer.MAX_VALUE);
    	} else if (algo.equals("encryption")) {
    		//encrypt and then default hash code
    		MessageDigest messageDigest = null;
			try {
				messageDigest = MessageDigest.getInstance("SHA-256");
			} catch (NoSuchAlgorithmException e) {
				assertFail("invalid encryption algorithm");
			}
    		messageDigest.update(value.getBytes());
    		String encryptedString = new String(messageDigest.digest());  
    		hashCode = encryptedString.hashCode();
    		if (hashCode < 0)
    			hashCode = - hashCode;
    	} else {
    		throw new IllegalArgumentException("invalid hash algorithms");
    	}
    	
    	return hashCode;
    }
 }
