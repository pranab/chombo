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

package org.chombo.distance;

import java.io.IOException;
import java.util.Map;

/**
 * Entity attributes are variable and dynamic
 * @author pranab
 *
 */
public abstract class DynamicVectorSimilarity {
	protected String fieldDelimRegex = "\\s+";
	protected boolean isBooleanVec;
	protected boolean isSemanticVec;
	protected boolean isCountIncluded;
	protected  int intersectionLength;
	protected String[] matchingContexts;
	
	
	/**
	 * @param src
	 * @param target
	 * @return
	 */
	public  double findDistance(String src, String target)  throws IOException {
		return 1.0;
	}
	
	public  double findDistance(String srcEntityID, String src, String targetEntityID, String target, String groupingID)  throws IOException {
		return 1.0;
	}
	
	/**
	 * @return
	 */
	public String getFieldDelimRegex() {
		return fieldDelimRegex;
	}

	/**
	 * @param fieldDelimRegex
	 */
	public void setFieldDelimRegex(String fieldDelimRegex) {
		this.fieldDelimRegex = fieldDelimRegex;
	}

	/**
	 * @return
	 */
	public boolean isBooleanVec() {
		return isBooleanVec;
	}

	/**
	 * @param isBooleanVec
	 */
	public void setBooleanVec(boolean isBooleanVec) {
		this.isBooleanVec = isBooleanVec;
	}
	
	/**
	 * @return
	 */
	public boolean isSemanticVec() {
		return isSemanticVec;
	}

	/**
	 * @param isSemanticVec
	 */
	public void setSemanticVec(boolean isSemanticVec) {
		this.isSemanticVec = isSemanticVec;
	}

	/**
	 * @return
	 */
	public boolean isCountIncluded() {
		return isCountIncluded;
	}

	/**
	 * @param isCountIncluded
	 */
	public void setCountIncluded(boolean isCountIncluded) {
		this.isCountIncluded = isCountIncluded;
	}
	
	/**
	 * @return
	 */
	public int getIntersectionLength() {
		return intersectionLength;
	}

	public String[] getMatchingContexts() {
		return matchingContexts;
	}

	/**
	 * @param algorithm
	 * @param params
	 * @return
	 * @throws IOException 
	 */
	public static DynamicVectorSimilarity createSimilarityStrategy(AttributeDistance attrDist) 
		throws IOException {
		String simAlgorithm = attrDist.getTextSimilarityStrategy();
		DynamicVectorSimilarity  simStrategy = null;
		if (simAlgorithm.equals("jaccard")){
			double srcNonMatchingTermWeight = attrDist.getJaccardSrcNonMatchingTermWeight();
			double trgNonMatchingTermWeight = attrDist.getJaccardTrgNonMatchingTermWeight();
			simStrategy = new JaccardSimilarity(srcNonMatchingTermWeight, trgNonMatchingTermWeight);
		} else if (simAlgorithm.equals("dice")){
			simStrategy = new DiceSimilarity();
		} else if (simAlgorithm.equals("charPair")){
			simStrategy = new CharacterPairSimilarity();
		} else if (simAlgorithm.equals("cosine")){
			simStrategy = new CosineSimilarity();
		} else {
			throw new IllegalArgumentException("invalid text similarity algorithms:" + simAlgorithm);
		}
		return simStrategy;
	}	
	
}
