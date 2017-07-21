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
import java.util.Map;

/**
 * @author pranab
 *
 */
public class AttributeFilter extends BaseAttributeFilter {
	private List<List<BasePredicate>> disjunctPredicates = new ArrayList<List<BasePredicate>>();
	public static final String CONJUNCT_SEP = " and ";
	public static final String DISJUNCT_SEP = " or ";
	private static String conjunctSeparator;
	private static String disjunctSeparator;
	
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
		AttributePredicate  predicate = null;
		String[] preds = filter.split(getConjunctSeparator());
		for (String pred : preds) {
			String[] predParts = pred.trim().split(AttributePredicate.PREDICATE_SEP);
			int compSize = predParts.length;
			if (compSize == 3) {
				//relational predicate
				int attr = Integer.parseInt(predParts[0]);
				String[] valueParts  = predParts[2].split(AttributePredicate.DATA_TYPE_SEP);
				
				if (valueParts[0].equals(BaseAttribute.DATA_TYPE_INT)) {
					predicate = new IntAttributePredicate(attr, predParts[1], valueParts[1]);
				} else if (valueParts[0].equals(BaseAttribute.DATA_TYPE_DOUBLE)) {
					predicate = new DoubleAttributePredicate(attr, predParts[1], valueParts[1]);
				} else if (valueParts[0].equals(BaseAttribute.DATA_TYPE_STRING)) {
					if (null != context) {
						predicate = new StringAttributePredicate();
						predicate.withContext(context);
						predicate.build(attr, predParts[1], valueParts[1]);
					} else {
						predicate = new StringAttributePredicate(attr, predParts[1], valueParts[1]);
					}
				} else {
					throw new IllegalArgumentException("invalid data type");
				}
				predicates.add(predicate);
			} else if (compSize == 1) {
				//udf
				String udf = pred.substring(0, pred.length()-2);
				String key = "pro.filter.udf.class." + udf;
				String udfClassName = (String)context.get(key);
				try {
					Class<?> cls = Class.forName(udfClassName);
					BasePredicate udfPredicate = (BasePredicate)cls.newInstance();
					udfPredicate.withContext(context);
				} catch (Exception ex) {
					throw new IllegalStateException("failed create filter udf object "+ ex.getMessage());
				}
				predicates.add(predicate);
			} else {
				throw new IllegalStateException("invalid predicate");
			}
		}
		
		return predicates;
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
