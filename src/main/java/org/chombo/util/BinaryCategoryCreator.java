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
 * Creates binary category from numeric data
 * @author pranab
 *
 */
public class BinaryCategoryCreator {
	private long threshold;
	private String lowerToken;
	private String upperToken;
	
	public BinaryCategoryCreator() {
	}

	public BinaryCategoryCreator(long threshold, String lowerToken, String upperToken) {
		super();
		this.threshold = threshold;
		this.lowerToken = lowerToken;
		this.upperToken = upperToken;
	}

	public long getThreshold() {
		return threshold;
	}

	public void setThreshold(long threshold) {
		this.threshold = threshold;
	}

	public String getLowerToken() {
		return lowerToken;
	}

	public void setLowerToken(String lowerToken) {
		this.lowerToken = lowerToken;
	}

	public String getUpperToken() {
		return upperToken;
	}

	public void setUpperToken(String upperToken) {
		this.upperToken = upperToken;
	}
	
	public String findToken(long value) {
		String token = value >= threshold ? upperToken : lowerToken;
		return token;
	}
	
}
