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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dynamic bean including list and map properties
 * @author pranab
 *
 */
public class DynamicBean {
	private Map<String, Object> map = new HashMap<String, Object>();

	/**
	 * @param name
	 * @param key
	 * @return
	 */
	public boolean contains(String name,  String key) {
		Map<String, Object> chMap = (Map<String, Object>)map.get(name);
		return chMap.containsKey(key);
	}

	/**
	 * @param name
	 * @return
	 */
	public Object get(String name) {
		return map.get(name);
	}

	/**
	 * @param name
	 * @return
	 */
	public String getString(String name) {
		return (String)map.get(name);
	}
	
	/**
	 * @param name
	 * @return
	 */
	public int getInt(String name) {
		return (Integer)map.get(name);
	}

	/**
	 * @param name
	 * @return
	 */
	public long getLong(String name) {
		return (Long)map.get(name);
	}

	/**
	 * @param name
	 * @return
	 */
	public double getDouble(String name) {
		return (Double)map.get(name);
	}

	/**
	 * @param name
	 * @param index
	 * @return
	 */
	public Object get(String name, int index) {
		Object value = null;
		List<Object> chList = (List<Object>)map.get(name);
		if (null != chList && index < chList.size()) {
			value = chList.get(index);
		}
		return value;
	}

	/**
	 * @param name
	 * @param index
	 * @return
	 */
	public String getString(String name, int index) {
		return (String)get( name,  index);
	}
	
	/**
	 * @param name
	 * @param index
	 * @return
	 */
	public Integer getInt(String name, int index) {
		return (Integer)get( name,  index);
	}

	/**
	 * @param name
	 * @param index
	 * @return
	 */
	public Long getLong(String name, int index) {
		return (Long)get( name,  index);
	}

	/**
	 * @param name
	 * @param index
	 * @return
	 */
	public Double getDouble(String name, int index) {
		return (Double)get( name,  index);
	}

	/**
	 * @param name
	 * @param key
	 * @return
	 */
	public Object get(String name, String key) {
		Object value = null;
		Map<String, Object> chMap = (Map<String, Object>)map.get(name);
		if (null != chMap) {
			value = chMap.get(key);
		}
		return value;
	}

	/**
	 * @param name
	 * @param key
	 * @return
	 */
	public String getString(String name, String key) {
		return (String)get(name,  key);
	}
	
	/**
	 * @param name
	 * @param key
	 * @return
	 */
	public int getInt(String name, String key) {
		return (Integer)get(name,  key);
	}

	/**
	 * @param name
	 * @param key
	 * @return
	 */
	public long getLong(String name, String key) {
		return (Long)get(name,  key);
	}

	/**
	 * @param name
	 * @param key
	 * @return
	 */
	public double getDouble(String name, String key) {
		return (Double)get(name,  key);
	}

	/**
	 * @param name
	 * @param key
	 */
	public void remove(String name, String key) {
	}

	/**
	 * @param name
	 * @param value
	 */
	public void set(String name, Object value) {
		map.put(name, value);
	}

	/**
	 * @param name
	 * @param index
	 * @param value
	 */
	public void set(String name, int index, Object value) {
		List<Object> chList = (List<Object>)map.get(name);
		if (null == chList) {
			chList = new ArrayList<Object>();
			map.put(name, chList);
		}
		chList.add(index, value);
	}

	/**
	 * @param name
	 * @param key
	 * @param value
	 */
	public void set(String name, String key, Object value) {
		Map<String, Object> chMap = (Map<String, Object>)map.get(name);
		if (null == chMap) {
			chMap = new HashMap<String, Object>();
			map.put(name, chMap);
		}
		chMap.put(key, value);
	}
	
	/**
	 * @return
	 */
	public Map<String, Object> getMap() {
		return map;
	}

}
