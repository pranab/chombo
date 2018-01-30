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

/**
 * @author pranab
 *
 */
public class Record {
	private String[] items;
	private int offset;
	private int precision = 3;
	protected String delim = ",";

	/**
	 * @param record
	 * @param delim
	 */
	public Record(String record, String delim) {
		this.items = record.split(delim, -1);
	}
	
	/**
	 * for accessing elements
	 * @param items
	 */
	public Record(String[] items) {
		this.items = items;
	}
	
	/**
	 * @param size
	 */
	public Record(int size) {
		this.items = new String[size];
	}

	/**
	 * @param items
	 */
	public void initialize(String[] items) {
		this.items = items;
		offset = 0;
	}
	
	/**
	 * @param delim
	 * @return
	 */
	public Record withDelim(String delim) {
		this.delim = delim;
		return this;
	}
	
	/**
	 * @param precision
	 * @return
	 */
	public Record withPrecision(int precision) {
		this.precision = precision;
		return this;
	}
	
	/**
	 * @return
	 */
	public int getSize() {
		return items.length;
	}
	
	/**
	 * @return
	 */
	public String getString() {
		return items[offset++];
	}
	
	/**
	 * @param value
	 */
	public void setString(String value) {
		items[offset++] = value;
	}

	/**
	 * @return
	 */
	public int getInt() {
		return Integer.parseInt(items[offset++]);
	}

	/**
	 * @param value
	 */
	public void setInt(int value) {
		items[offset++] = "" + value;
	}

	/**
	 * @return
	 */
	public long getLong() {
		return Long.parseLong(items[offset++]);
	}

	/**
	 * @param value
	 */
	public void setLong(long value) {
		items[offset++] = "" + value;
	}

	/**
	 * @return
	 */
	public float getFloat() {
		return Float.parseFloat(items[offset++]);
	}
	
	/**
	 * @return
	 */
	public double getDouble() {
		return Double.parseDouble(items[offset++]);
	}
	/**
	 * @param value
	 */
	public void setDouble(double value) {
		items[offset++] = BasicUtils.formatDouble(value, precision);
	}
	
	public String toString() {
		return BasicUtils.join(items, delim);
	}
}
