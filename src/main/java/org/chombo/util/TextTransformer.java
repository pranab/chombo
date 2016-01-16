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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.util.Version;

/**
 * @author pranab
 *
 */
public class TextTransformer {
	private String[] excludeRegexes;
	private String[] includeRegexes;
	private Pattern[] includePatterns;
	private boolean doAnalyze;
    private Analyzer analyzer;
	
	public  TextTransformer() {
		
	}
	
	public TextTransformer withExcludeRegexes(String[] excludeRegexes) {
		this.excludeRegexes = excludeRegexes;
		return this;
	}
	
	public TextTransformer withIncludeRegexes(String[] includeRegexes) {
		this.includeRegexes = includeRegexes;
		includePatterns = new Pattern[includeRegexes.length];
		int i = 0;
		for (String regex :  includeRegexes) {
			includePatterns[i++] = Pattern.compile(regex);
		}
		
		return this;
	}
	
	/**
	 * @param doAnalyze
	 * @param lang
	 * @return
	 */
	public TextTransformer withDoAnalyze(boolean doAnalyze, String lang) {
		this.doAnalyze = doAnalyze;
    	if (lang.equals("en")) {
    		analyzer = new EnglishAnalyzer(Version.LUCENE_44);
    	}  else {
			throw new IllegalArgumentException("unsupported language:" + lang);
		} 

		return this;
	}
	
	/**
	 * @param record
	 * @return
	 * @throws IOException 
	 */
	public String  process(String record) throws IOException {
		String processed = record;
		if (null != excludeRegexes) {
			for (String regex : excludeRegexes) {
				processed = processed.replaceAll(regex, "");
			}
		} else if (null != includeRegexes) {
			List<String> matchedTokens = new ArrayList<String>();
			String[] items = record.split("\\s+");
			for (String item : items) {
				for (Pattern pattern : includePatterns) {
					if (pattern.matcher(item).matches()) {
						matchedTokens.add(item);
						break;
					}
				}
			}
			processed = Utility.join(matchedTokens, " ");
		}

		if (doAnalyze) {
			processed = Utility.analyze(processed, analyzer);
		}
		
		return processed;
	}
	
}
