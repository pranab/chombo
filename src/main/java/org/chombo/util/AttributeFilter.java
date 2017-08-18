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
import java.util.List;
import java.util.Map;

/**
 * @author pranab
 *
 */
public class AttributeFilter extends BaseAttributeFilter {
	private List<List<BasePredicate>> disjunctPredicates = new ArrayList<List<BasePredicate>>();
	private String operandDataType;
	public static final String CONJUNCT_SEP = " and ";
	public static final String DISJUNCT_SEP = " or ";
	private static String conjunctSeparator;
	private static String disjunctSeparator;
	private static final String FIELD_PREFIX = "$";
	
	public AttributeFilter(){
	}

	/**
	 * @param filter
	 */
	public AttributeFilter(String filter) {
		build(filter);
	}
	
	/**
	 * @param filter
	 */
	public AttributeFilter(String filter, Map<String, Object> context) {
		this.context = context;
		build(filter);
	}

	/* (non-Javadoc)
	 * @see org.chombo.util.BaseAttributeFilter#build(java.lang.String)
	 */
	public void build(String filter) {
		String[] conjuctPreds = filter.split(getDisjunctSeparator());
		
		//all conjunctive predicates
		for (String conjuctPred : conjuctPreds) {
			disjunctPredicates.add(buildConjuctPredicate(conjuctPred.trim()));
		}
	}
	
	/**
	 * @param filter
	 */
	private List<BasePredicate> buildConjuctPredicate(String filter) {
		List<BasePredicate> predicates = new ArrayList<BasePredicate>();
		BasePredicate  predicate = null;
		String[] preds = filter.split(getConjunctSeparator());
		for (String pred : preds) {
			String[] predParts = pred.trim().split(AttributePredicate.PREDICATE_SEP);
			int compSize = predParts.length;
			if (compSize == 3) {
				//relational predicate
				int attr = getAttrOrd(predParts[0]);
				//String[] valueParts  = predParts[2].split(AttributePredicate.DATA_TYPE_SEP);
				
				//value and data type
				ValueType valType = getValue(predParts[2]);
				
				if (valType.getType().equals(BaseAttribute.DATA_TYPE_INT)) {
					predicate = new IntAttributePredicate(attr, predParts[1], valType.getStringValue());
				} else if (valType.getType().equals(BaseAttribute.DATA_TYPE_DOUBLE)) {
					predicate = new DoubleAttributePredicate(attr, predParts[1], valType.getStringValue());
				} else if (valType.getType().equals(BaseAttribute.DATA_TYPE_STRING)) {
					if (null != context) {
						predicate = new StringAttributePredicate();
						predicate.withContext(context);
						predicate.build(attr, predParts[1], valType.getStringValue());
					} else {
						predicate = new StringAttributePredicate(attr, predParts[1], valType.getStringValue());
					}
				} else {
					throw new IllegalArgumentException("invalid data type");
				}
				predicates.add(predicate);
			} else if (compSize == 1) {
				//udf
				String udf = predParts[0].substring(0, predParts[0].length()-2);
				String key = "pro.filter.udf.class." + udf;
				String udfClassName = (String)context.get(key);
				//System.out.println("udf:" +udf + "  class:" +udfClassName );
				BasePredicate udfPredicate = null;
				try {
					Class<?> cls = Class.forName(udfClassName);
					udfPredicate = (BasePredicate)cls.newInstance();
					udfPredicate.withContext(context);
				} catch (Exception ex) {
					throw new IllegalStateException("failed create filter udf object "+ ex.getMessage());
				}
				predicates.add(udfPredicate);
			} else {
				throw new IllegalStateException("invalid predicate  format");
			}
		}
		
		return predicates;
	}
	
	/**
	 * @param field
	 * @return
	 */
	private int getAttrOrd(String attrName) {
		int attr = 0;
		if (attrName.startsWith(FIELD_PREFIX)) {
			attrName = attrName.substring(1);
		}
		
		if (BasicUtils.isInt(attrName)) {
			//by index
			attr = Integer.parseInt(attrName);
		} else {
			//by name using schema to get ordinal
			BaseAttribute attrib = getAttribute(attrName);
			attr = attrib.getOrdinal();
			operandDataType = attrib.getDataType();
		}
		
		return attr;
	}
	
	/**
	 * @param predParts
	 * @return
	 */
	private ValueType getValue(String value) {
		ValueType valueType = null;
		String[] valueParts  = value.split(AttributePredicate.DATA_TYPE_SEP);
		if (valueParts.length == 2) {
			//embedded data type
			valueType = new ValueType(valueParts[1], valueParts[0]);
		} else {
			//get data type from schema
			valueType = new ValueType(valueParts[0], operandDataType);
		}
		return valueType;
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.util.BaseAttributeFilter#evaluate(java.lang.String[])
	 */
	public boolean evaluate(String[] record) {
		boolean status = false;
		for (List<BasePredicate> predicates : disjunctPredicates) {
			status = status || evaluateConjuctPredicates(record, predicates);
			if (status) {
				//conjunctive predicates or connected
				break;
			}
		}
		
		return status;
	}
	
	/**
	 * evaluate conjunctive predicates
	 * @param record
	 * @return
	 */
	private boolean evaluateConjuctPredicates(String[] record, List<BasePredicate> predicates) {
		boolean status = true;
		for (BasePredicate  predicate : predicates) {
			status = status & predicate.evaluate(record);
			if (!status) {
				//predicates and connected
				break;
			}
		}
		
		return status;
	}
	
	/**
	 * @return
	 */
	public static String getConjunctSeparator() {
		return conjunctSeparator != null ? conjunctSeparator : CONJUNCT_SEP;
	}

	/**
	 * @param conjunctSeparator
	 */
	public static void setConjunctSeparator(String conjunctSeparator) {
		AttributeFilter.conjunctSeparator = conjunctSeparator;
	}
	
	/**
	 * @return
	 */
	public static String getDisjunctSeparator() {
		return disjunctSeparator != null ? disjunctSeparator : DISJUNCT_SEP;
	}

	/**
	 * @param disjunctSeparator
	 */
	public static void setDisjunctSeparator(String disjunctSeparator) {
		AttributeFilter.disjunctSeparator = disjunctSeparator;
	}

	/**
	 * @param filter
	 * @return
	 */
	public static boolean isConjuctivePredicate(String filter) {
		String[] preds = filter.split(getConjunctSeparator());
		String[] predParts = preds[0].trim().split(AttributePredicate.PREDICATE_SEP);
		return predParts.length == 3;
	}
	
	
	
}
