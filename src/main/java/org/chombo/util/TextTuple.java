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

import org.apache.hadoop.io.Text;

/**
 * Tuple encoded in Text
 * @author pranab
 *
 */
public class TextTuple extends Text {
	 private static final String DELIM = ",";
	 private String[] items;
	 private String fieldDelim = DELIM;
	 
	/**
	 * constructor
	 */
	public TextTuple() {
		super();
	}

	/**
	 * constructor
	 * @param utf8
	 */
	public TextTuple(byte[] utf8) {
		super(utf8);
	}

	/**
	 * constructor
	 * @param string
	 */
	public TextTuple(String string) {
		super(string);
	}

	/**
	 * constructor
	 * @param utf8
	 */
	public TextTuple(Text utf8) {
		super(utf8);
	}
	
	/**
	 * @param fieldDelim
	 */
	public void setFieldDelim(String fieldDelim) {
		this.fieldDelim = fieldDelim;
	}

	/**
	 * add one or more elements
	 * @param fieldList
	 */
	public void add(Object...  fieldList) {
		StringBuilder stBld = new  StringBuilder();
		for (Object field :  fieldList) {
			stBld.append(field).append(fieldDelim);
		}
		this.set(stBld.substring(0,stBld.length() - 1));
	}
	
	/**
	 * splits into tokens
	 */
	public void prepareForRead() {
		items = this.toString().split(fieldDelim);
	}

	/**
	 * @param index
	 * @return
	 */
	public String getString(int index) {
		return items[index];
	}

	/**
	 * @param index
	 * @return
	 */
	public int getInt(int index) {
		return Integer.parseInt(items[index]);
	}
	
	/**
	 * @param index
	 * @return
	 */
	public long getLong(int index) {
		return Long.parseLong(items[index]);
	}

	/**
	 * @param index
	 * @return
	 */
	public double getDouble(int index) {
		return Double.parseDouble(items[index]);
	}

	public void initialize() {
		this.clear();
		items = null;
	}
}
