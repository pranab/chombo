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
	 
	public TextTuple() {
		super();
	}

	public TextTuple(byte[] utf8) {
		super(utf8);
	}

	public TextTuple(String string) {
		super(string);
	}

	public TextTuple(Text utf8) {
		super(utf8);
	}
	
	/**
	 * add one or more elements
	 * @param fieldList
	 */
	public void add(Object...  fieldList) {
		StringBuilder stBld = new  StringBuilder();
		for (Object field :  fieldList) {
			stBld.append(field).append(DELIM);
		}
		this.set(stBld.substring(0,stBld.length() - 1));
	}

	/**
	 * @param index
	 * @return
	 */
	public String getString(int index) {
		if (null == items) {
			items = this.toString().split(DELIM);
		}
		return items[index];
	}

	/**
	 * @param index
	 * @return
	 */
	public int getInt(int index) {
		if (null == items) {
			items = this.toString().split(DELIM);
		}
		return Integer.parseInt(items[index]);
	}
	/**
	 * @param index
	 * @return
	 */
	public double getDouble(int index) {
		if (null == items) {
			items = this.toString().split(DELIM);
		}
		return Double.parseDouble(items[index]);
	}

}
