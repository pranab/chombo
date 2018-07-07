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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.chombo.util.Attribute;
import org.chombo.util.BasicUtils;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.Pair;
import org.chombo.util.RichAttribute;
import org.chombo.util.RichAttributeSchema;

/**
 * @author pranab
 *
 */
public class InterRecordDistance implements Serializable {
	private GenericAttributeSchema attrSchema;
	private AttributeDistanceSchema attrDistSchema;
	private String fieldDelim;
	private String subFieldDelim = BasicUtils.DEF_SUB_FIELD_DELIM;
	private String[] firstItems;
	private String[] secondItems;
	private String firstItem;
	private String secondItem;
	private int ordinal;
	private Map<Integer, Double> attrDistances = new HashMap<Integer, Double>();
	private Map<Integer, DynamicVectorSimilarity> textSimilarityStrategies = 
			new HashMap<Integer, DynamicVectorSimilarity>();
	private boolean doubleRange;
	private boolean categoricalSet;
	private int fieldOrd;
	protected Map<Integer, Map<Pair<String, String>, Double>> valueDiffMetricDist = 
			new HashMap<Integer, Map<Pair<String, String>, Double>>();
	private int scale = 1;
	private int[] facetedFields;
	private boolean tolerateMissingValue;
	
	/**
	 * @param attrSchema
	 * @param attrDistSchema
	 * @param fieldDelim
	 */
	public InterRecordDistance(GenericAttributeSchema attrSchema,
			AttributeDistanceSchema attrDistSchema, String fieldDelim) {
		this.attrSchema = attrSchema;
		this.attrDistSchema = attrDistSchema;
		this.fieldDelim = fieldDelim;
	}
	
	/**
	 * @param scale
	 * @return
	 */
	public InterRecordDistance withScale(int scale) {
		this.scale = scale;
		return this;
	}
	
	/**
	 * @param facetedFields
	 * @return
	 */
	public InterRecordDistance withFacetedFields(int[] facetedFields) {
		this.facetedFields = facetedFields;
		return this;
	}
	
	/**
	 * @param subFieldDelim
	 * @return
	 */
	public InterRecordDistance withSubFieldDelim(String subFieldDelim) {
		this.subFieldDelim = subFieldDelim;
		return this;
	}
	
	/**
	 * @param doubleRange
	 * @return
	 */
	public InterRecordDistance withDoubleRange(boolean doubleRange) {
		this.doubleRange = doubleRange;
		return this;
	}
	
	/**
	 * @param categoricalSet
	 * @return
	 */
	public InterRecordDistance withCategoricalSet(boolean categoricalSet) {
		this.categoricalSet = categoricalSet;
		return this;
	}
	
	/**
	 * @param tolerateMissingValue
	 * @return
	 */
	public InterRecordDistance withTolerateMissingValue(boolean tolerateMissingValue) {
		this.tolerateMissingValue = tolerateMissingValue;
		return this;
	}
	
	/**
	 * @param records
	 * @param idFieldLen
	 * @return
	 */
	public InterRecordDistance withValueDiffMetricDist(List<String[]> records, int idFieldLen) {
		for (String[] rec : records) {
			int offset = idFieldLen;
			int attrOrd = Integer.parseInt(rec[offset++]);
			Map<Pair<String, String>, Double> valuePairDist = new HashMap<Pair<String, String>, Double>();
			valueDiffMetricDist.put(attrOrd, valuePairDist);
			
			while (offset < rec.length) {
				String firstAttrVal = rec[offset++];
				String secAttrVal = rec[offset++];
				Pair<String,String> valPair = distWithAttrValuesSorted(firstAttrVal, secAttrVal);
				Double dist = Double.parseDouble(rec[offset++]);
				valuePairDist.put(valPair, dist);
			}
			
		}
		return this;
	}
	
	/**
	 * @param firstAttrVal
	 * @param secAttrVal
	 * @return
	 */
	private Pair<String,String> distWithAttrValuesSorted(String firstAttrVal, String secAttrVal) {
		Pair<String,String> valPair = null;
		if (firstAttrVal.compareTo(secAttrVal) > 0) {
			valPair = new Pair<String,String>(firstAttrVal, secAttrVal);
		} else {
			valPair = new Pair<String,String>(secAttrVal, firstAttrVal);
		}
		return valPair;
	}
	
	/**
	 * @param first
	 * @param second
	 * @return
	 * @throws IOException
	 */
	public int findScaledDistance(String first, String second) throws IOException {
		int dist = (int)(scale * findDistance(first, second));
		return dist;
	}
	
	/**
	 * @param first
	 * @param second
	 * @return
	 * @throws IOException 
	 */
	public double findDistance(String[] firstRec, String[] secondRec, int fieldOrd) throws IOException {
		this.fieldOrd = fieldOrd;
		return findDistance(secondRec[fieldOrd], secondRec[fieldOrd]);
	}	
	
	/**
	 * @param first
	 * @param second
	 * @return
	 * @throws IOException 
	 */
	public double findDistance(String  first, String second) throws IOException {
		String[] firstItems = first.split(fieldDelim);
		String[] secondItems = second.split(fieldDelim);
		return findDistance(firstItems, secondItems );
	}
	
	/**
	 * @param firstItems
	 * @param secondItems
	 * @return
	 * @throws IOException 
	 */
	public double findDistance(String[]  firstItems, String[]  secondItems) throws IOException {
		this.firstItems =firstItems;
		this.secondItems = secondItems;
		double recDist = 0;
		double dist = 0;
		attrDistances.clear();
		
		//attribute pair distances
		for (Attribute attr : attrSchema.getAttributes()) {
			ordinal = attr.getOrdinal();
			//skip if ID
			if (attr.isId())
				continue;
			
			//skip if not faceted field
			if (null != facetedFields) {
				//if faceted set but field not included, then skip it
				if (!ArrayUtils.contains(facetedFields, ordinal)) {
					continue;
				}
			}
			
			AttributeDistance attrDist = attrDistSchema.findAttributeDistanceByOrdinal(ordinal);
			firstItem = firstItems[ordinal];
			secondItem = secondItems[ordinal];
			
			//missing value
			boolean anyMissingValue = firstItem.isEmpty() || secondItem.isEmpty();
			if (!tolerateMissingValue && anyMissingValue) {
				throw new IllegalStateException("missing field found");
			}
			
			//only none of the fields has missing value
			if (anyMissingValue){
				continue;
			}
			
			dist = attributeDistance(attr, attrDist);
			attrDistances.put(ordinal, dist);
		}
		
		//aggregate distance, different groups of attributes can be aggregated by different algorithms
		double sumDist = 0;
		double sumWeight = 0;
		for (AttributeDistanceAggregator aggregator :  attrDistSchema.getAttrAggregators()) {
			if (aggregator.getAlgorithm().equals("euclidean")) {
				dist = aggregateEuclidean(aggregator.getOrdinals());
			} else if (aggregator.getAlgorithm().equals("manhattan")) {
				dist = aggregateManhattan(aggregator.getOrdinals());
			} else if (aggregator.getAlgorithm().equals("minkwoski")) {
				dist = aggregateMinkwoski(aggregator.getOrdinals(), aggregator.getParam());
			} else if (aggregator.getAlgorithm().equals("categorical")) {
				dist = aggregateCategorical(aggregator.getOrdinals());
			}
			sumDist += dist * aggregator.getWeight();
			sumWeight += aggregator.getWeight();
			
		}
		recDist = sumDist / sumWeight;
		return recDist;
	}
	
	/**
	 * @param attr
	 * @param attrDist
	 * @return
	 * @throws IOException
	 */
	private double attributeDistance(Attribute attr, AttributeDistance attrDist) throws IOException {
		double dist = 0;
		if (attr.isCategorical()) {
			dist = categoricalDistance(attr, attrDist);
		} else if (attr.isInteger()) {
			dist = numericDistance(Integer.parseInt(firstItem), Integer.parseInt(secondItem), attrDist);
		} else if (attr.isDouble()) {
			if (doubleRange) {
				DoubleRange firstItemRange = DoubleRange.create(firstItem, subFieldDelim);
				if (null != firstItemRange) {
					dist = numericDistance(firstItemRange,  Double.parseDouble(secondItem),  attrDist);
				} else {
					DoubleRange secondItemRange = DoubleRange.create(secondItem, subFieldDelim);
					if (null != secondItemRange) {
						dist = numericDistance(secondItemRange,  Double.parseDouble(firstItem),  attrDist);
					} else {
						throw new IllegalStateException("no range data found in field");
					}
				}
			} else {
				dist = numericDistance(Double.parseDouble(firstItem), Double.parseDouble(secondItem), attrDist);
			}
		} else if (attr.isText()) {
			dist = textDistance(attrDist);
		} else if (attr.isGeoLocation()) {
			dist = geoLocationDistance(attrDist);
		}
		
		return dist;
	}
	
	/**
	 * @param attr
	 * @param attrDist
	 * @return
	 */
	private double categoricalDistance(Attribute attr, AttributeDistance attrDist) {
		double dist = 0;
		if (attrDist.getAlgorithm().equals("cardinality")) {
			//cardinality
			dist = firstItem.equals(secondItem) ? 0 : Math.sqrt(2) / attr.getCardinality().size();
		} else if (attrDist.getAlgorithm().equals("valueDiffMetric")) {
			//value difference metric
			Pair<String,String> valPair = distWithAttrValuesSorted(firstItem, secondItem);
			dist = valueDiffMetricDist.get(fieldOrd).get(valPair);
		}else {
			//default equality or inclusion based
			if (categoricalSet) {
				List<String> firstList = BasicUtils.toList(firstItem.split(subFieldDelim));
				List<String> secondList = BasicUtils.toList(secondItem.split(subFieldDelim));
				dist = BasicUtils.listIncluded(firstList, secondList) ? 0 : 1;
			}else {
				dist = firstItem.equals(secondItem) ? 0 : 1;
			}
		}
		return dist;
	}
	
	/**
	 * @param firstItemDbl
	 * @param secondItemDbl
	 * @param attrDist
	 * @return
	 */
	private double numericDistance(double firstItemDbl, double secondItemDbl, AttributeDistance attrDist) {
		double dist = 0;
		dist = Math.abs(firstItemDbl - secondItemDbl);
		
		//apply weight
		if (attrDist.isWeightSet()) {
			dist /= attrDist.getWeight();
		}
		
		//apply threshold
		if (attrDist.isUpperThresholdSet() && dist > attrDist.getUpperThreshold()) {
			dist = 1;
		} else if (attrDist.isLowerThresholdSet() && dist < attrDist.getLowerThreshold()) {
			dist = 0;
		}
		return dist;
	}
	
	/**
	 * @param firstItemDblRange
	 * @param secondItemDbl
	 * @param attrDist
	 * @return
	 */
	private double numericDistance(DoubleRange firstItemDblRange, double secondItemDbl, AttributeDistance attrDist) {
		double dist = 0;
		if (secondItemDbl > firstItemDblRange.getUpper()) {
			dist = numericDistance(firstItemDblRange.getUpper(), secondItemDbl,  attrDist);
		} else if (secondItemDbl < firstItemDblRange.getLower()) {
			dist = numericDistance(secondItemDbl, firstItemDblRange.getLower(), attrDist);
		}
		return dist;
	}
	
	/**
	 * @param attrDist
	 * @return
	 * @throws IOException 
	 */
	private double textDistance(AttributeDistance attrDist) throws IOException {
		DynamicVectorSimilarity simStrategy = textSimilarityStrategies.get(ordinal);
		if (null == simStrategy) {
			simStrategy = DynamicVectorSimilarity.createSimilarityStrategy(attrDist);
			textSimilarityStrategies.put(ordinal, simStrategy);
		}
		return simStrategy.findDistance(firstItem, secondItem);
	}
	
	/**
	 * @param attrDist
	 * @return
	 */
	private double geoLocationDistance(AttributeDistance attrDist) {
    	String[] items = firstItem.split(subFieldDelim);
    	double lat1 = Double.parseDouble(items[0]);
    	double long1 = Double.parseDouble(items[1]);
    	
    	items = secondItem.split(":");
    	double lat2 = Double.parseDouble(items[0]);
    	double long2 = Double.parseDouble(items[1]);
		double dist = BasicUtils.getGeoDistance(lat1, long1, lat2, long2);
		if (attrDist.isMaxGeoDistanceSet()) {
			dist /= attrDist.getMaxGeoDistance();
		}
		
		return dist;
	}
	
	/**
	 * @param ordinals
	 * @return
	 */
	private double aggregateEuclidean(int[] ordinals) {
		double dist = 0;
		double sum = 0;
		int count = 0;
		for (int ordinal : ordinals) {
			Double attrDist = attrDistances.get(ordinal);
			if (null != attrDist) {
				sum += attrDist * attrDist;
				++count;
			}
		}
		dist = Math.sqrt(sum) / count;
		return dist;
	}
	
	/**
	 * attribute wise distance
	 * @return
	 */
	public Map<Integer, Double> getAttributeDistances() {
		return attrDistances;
	}
	
	/**
	 * @param ordinals
	 * @return
	 */
	private double aggregateManhattan(int[] ordinals) {
		double dist = 0;
		double sum = 0;
		int count = 0;
		for (int ordinal : ordinals) {
			Double attrDist = attrDistances.get(ordinal);
			if (null != attrDist) {
				sum += attrDist;
				++count;
			}
		}
		dist = sum / count;
		return dist;
	}
	
	/**
	 * @param ordinals
	 * @return
	 */
	private double aggregateMinkwoski(int[] ordinals, double param) {
		double dist = 0;
		double sum = 0;
		int count = 0;
		for (int ordinal : ordinals) {
			Double attrDist = attrDistances.get(ordinal);
			if (null != attrDist) {
				sum += Math.pow(attrDist, param);
				++count;
			}
		}
		
		dist = Math.pow(sum, 1.0/param) / count;
		return dist;
	}

	/**
	 * @param ordinals
	 * @return
	 */
	private double aggregateCategorical(int[] ordinals) {
		double dist = 0;
		double sum = 0;
		int count = 0;
		for (int ordinal : ordinals) {
			Double attrDist = attrDistances.get(ordinal);
			if (null != attrDist) {
				sum += attrDist;
				++count;
			}
		}
		dist = sum / count;
		return dist;
	}
	
	/**
	 * @author pranab
	 *
	 */
	private static class DoubleRange extends Pair<Double, Double> {
		
		/**
		 * @param first
		 * @param second
		 */
		public DoubleRange(Double first, Double second) {
			super(first,  second);
		}
		
		/**
		 * @return
		 */
		public double getLower() {
			return getLeft();
		}
		
		/**
		 * @return
		 */
		public double getUpper() {
			return getRight();
		}

		/**
		 * @param field
		 * @param subFieldDelim
		 * @return
		 */
		public static DoubleRange create(String field, String subFieldDelim) {
			DoubleRange doubleRange = null;
			String[] items = field.split(subFieldDelim);
			if (items.length == 2) {
				doubleRange = new DoubleRange(Double.parseDouble(items[0]), Double.parseDouble(items[1]));
			} else if (items.length != 1){
				throw new IllegalStateException("too many sub fields");
			}
			
			return doubleRange;
		}
		
	}
}
