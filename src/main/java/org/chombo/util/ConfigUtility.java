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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Map or Properties based configuration related utility
 * @author pranab
 *
 */
public  class ConfigUtility {
	
	public static boolean has(Map conf, String key) {
		return conf.containsKey(key);
	}
	
	/**
	 * @param conf
	 * @param key
	 * @return
	 */
	public static String getString(Map conf, String key) {
		String val = null;
		Object obj = conf.get(key);
		if (null != obj) {
			if (obj instanceof String){
				val = (String)obj;
			} else {
				throw new IllegalArgumentException("String value not found  in configuration  for " + key);
			}
		} else {
			throw new IllegalArgumentException("Nothing found in configuration  for " + key);
		}
		return val;
	}

	/**
	 * @param conf
	 * @param key
	 * @param def
	 * @return
	 */
	public static String getString(Map conf,String key, String def) {
		String val = null;
		try {
			val = getString( conf, key);
		} catch (IllegalArgumentException ex) {
			val = def;
		}
		return val;
	}

	/**
	 * @param conf
	 * @param key
	 * @return
	 */
	public static int getInt(Map conf,String key) {
		int val = 0;
		Object obj = conf.get(key);
		if (null != obj) {
			if (obj instanceof Integer) {
				val = (Integer)obj;
			} else if (obj instanceof String) {
				val = Integer.parseInt((String)obj);
			} else {
				throw new IllegalArgumentException("String value not found  in configuration  for " + key);
			}
		} else {
			throw new IllegalArgumentException("Nothing found in configuration for " + key);
		}
		return val;
	}
	
	/**
	 * @param conf
	 * @param key
	 * @return
	 */
	public static int[] getIntArray(Map conf,String key) {
		int[] values = null;
		Object obj = conf.get(key);
		if (null != obj) {
			if (obj instanceof int[]) {
				values = (int[])obj;
			} else if (obj instanceof String) {
				String[] items  = ((String)obj).split(",");
				values = new int[items.length];
				for (int i = 0; i < items.length; ++i) {
					values[i] = Integer.parseInt(items[i]);
				}
			} else {
				throw new IllegalArgumentException("String value not found  in configuration  for " + key);
			}
		} else {
			throw new IllegalArgumentException("Nothing found in configuration for " + key);
		}
		return values;
	}
	
	/**
	 * @param conf
	 * @param key
	 * @return
	 */
	public static float[] getFloatArray(Map conf,String key) {
		float[] values = null;
		Object obj = conf.get(key);
		if (null != obj) {
			if (obj instanceof float[]) {
				values = (float[])obj;
			} else if (obj instanceof String) {
				String[] items  = ((String)obj).split(",");
				values = new float[items.length];
				for (int i = 0; i < items.length; ++i) {
					values[i] = Float.parseFloat(items[i]);
				}
			} else {
				throw new IllegalArgumentException("String value not found  in configuration  for " + key);
			}
		} else {
			throw new IllegalArgumentException("Nothing found in configuration for " + key);
		}
		return values;
	}
	

	/**
	 * @param conf
	 * @param key
	 * @return
	 */
	public static double[] getDoubleArray(Map conf,String key) {
		double[] values = null;
		Object obj = conf.get(key);
		if (null != obj) {
			if (obj instanceof double[]) {
				values = (double[])obj;
			} else if (obj instanceof String) {
				String[] items  = ((String)obj).split(",");
				values = new double[items.length];
				for (int i = 0; i < items.length; ++i) {
					values[i] = Double.parseDouble(items[i]);
				}
			} else {
				throw new IllegalArgumentException("String value not found  in configuration  for " + key);
			}
		} else {
			throw new IllegalArgumentException("Nothing found in configuration for " + key);
		}
		return values;
	}

	/**
	 * @param conf
	 * @param key
	 * @return
	 */
	public static long getLong(Map conf,String key) {
		long val = 0;
		Object obj = conf.get(key);
		if (null != obj) {
			if (obj instanceof Long) {
				val = (Long)obj;
			} else if (obj instanceof String) {
				val = Long.parseLong((String)obj);
			} else {
				throw new IllegalArgumentException("String value not found  in configuration  for " + key);
			}
		} else {
			throw new IllegalArgumentException("Nothing found in configuration for " + key);
		}
		return val;
	}
	
	/**
	 * @param conf
	 * @param key
	 * @param def
	 * @return
	 */
	public static int getInt(Map conf,String key, int def) {
		int val = 0;
		try {
			val = getInt(conf,  key);
		} catch (Exception ex) {
			val = def;
		}
		return val;
	}
	
	/**
	 * @param conf
	 * @param key
	 * @param def
	 * @return
	 */
	public static long getLong(Map conf,String key, long def) {
		long val = 0;
		try {
			val = getLong(conf,  key);
		} catch (Exception ex) {
			val = def;
		}
		return val;
	}

	/**
	 * @param conf
	 * @param key
	 * @return
	 */
	public static float getFloat(Map conf,String key) {
		float  val = 0;
		Object obj = conf.get(key);
		if (null != obj) {
			if (obj instanceof Float) {
				val = (Float)obj;
			} else if (obj instanceof String) {
				val = Float.parseFloat((String)obj);
			} else {
				throw new IllegalArgumentException("String value not found  in configuration  for " + key);
			}
		} else {
			throw new IllegalArgumentException("Nothing found in configuration for " + key);
		}
		return val;
	}
	
	/**
	 * @param conf
	 * @param key
	 * @return
	 */
	public static double getDouble(Map conf,String key) {
		double  val = 0;
		Object obj = conf.get(key);
		if (null != obj) {
			if (obj instanceof Double) {
				val = (Double)obj;
			} else if (obj instanceof String) {
				val = Double.parseDouble((String)obj);
			} else {
				throw new IllegalArgumentException("String value not found  in configuration  for " + key);
			}
		} else {
			throw new IllegalArgumentException("Nothing found in configuration for " + key);
		}
		return val;
	}

	/**
	 * @param conf
	 * @param key
	 * @param def
	 * @return
	 */
	public static double getDouble(Map conf,String key, double  def) {
		double val = 0;
		try {
			val = getDouble(conf,  key);
		} catch (Exception ex) {
			val = def;
		}
		return val;
	}
	
	/**
	 * @param conf
	 * @param key
	 * @return
	 */
	public static boolean getBoolean(Map conf,String key) {
		boolean val = false;
		Object obj = conf.get(key);
		if (null != obj) {
			if (obj instanceof Boolean) {
				val = (Boolean)obj;
			} else if (obj instanceof String) {
				val = Boolean.parseBoolean((String)obj);
			} else {
				throw new IllegalArgumentException("Boolean value not found  in configuration  for " + key);
			}
		} else {
			throw new IllegalArgumentException("Nothing found in configuration for " + key);
		}
		return val;
	}
	
	/**
	 * @param conf
	 * @param key
	 * @param def
	 * @return
	 */
	public static boolean getBoolean(Map conf,String key, boolean def) {
		boolean val = false;
		try {
			val = getBoolean(conf,  key);
		} catch (Exception ex) {
			val = def;
		}
		return val;
	}

	/**
	 * @param conf
	 * @param key
	 * @return
	 */
	public static boolean exists(Map conf,String key) {
		return conf.get(key) != null;
	}
	
	/**
	 * @param conf
	 * @return
	 */
	public static Map<String, Object> toTypedMap(Map conf) {
		Map<String, Object> typedConf = new HashMap<String, Object>();
		for (Object key : conf.keySet()) {
			typedConf.put((String)key, conf.get(key));
		}
		return typedConf;
	}

	/**
	 * @param props
	 * @param key
	 * @return
	 */
	public static String getString(Properties props, String key) {
		String val = props.getProperty(key);
		return val;
	}

	/**
	 * @param props
	 * @param key
	 * @return
	 */
	public static String getString(Properties props, String key, String defVal) {
		String val = props.getProperty(key, defVal);
		return val;
	}

	/**
	 * @param props
	 * @param key
	 * @return
	 */
	public static int getInt(Properties props, String key) {
		int val = 0;
		String valSt = props.getProperty(key);
		if (null != valSt) {
			val = Integer.parseInt(valSt);
		} else {
			throw new IllegalArgumentException("value not found  in configuration  for " + key);
		}
		return val;
	}

	/**
	 * @param props
	 * @param key
	 * @param defVal
	 * @return
	 */
	public static int geInt(Properties props, String key, int defVal) {
		int val = 0;
		try {
		val = getInt(props, key);
		} catch (Exception ex) {
			val = defVal;
		}
		return val;
	}

	/**
	 * @param props
	 * @param key
	 * @return
	 */
	public static boolean getBoolean(Properties props, String key) {
		boolean val = false;
		String valSt = props.getProperty(key);
		if (null != valSt) {
			val =Boolean.parseBoolean(valSt);
		} else {
			throw new IllegalArgumentException("value not found  in configuration  for " + key);
		}
		return val;
	}

	/**
	 * @param props
	 * @param key
	 * @param defVal
	 * @return
	 */
	public static boolean geBoolean(Properties props, String key, boolean defVal) {
		boolean  val = false;
		try {
			val = getBoolean(props, key);
		} catch (Exception ex) {
			val = defVal;
		}
		return val;
	}

}
