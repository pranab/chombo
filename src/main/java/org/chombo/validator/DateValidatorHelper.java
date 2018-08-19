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

package org.chombo.validator;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.chombo.util.BasicUtils;
import org.chombo.util.ProcessorAttribute;

/**
 * @author pranab
 *
 */
public class DateValidatorHelper implements Serializable {
	private boolean epochTimeMs;
	private SafeDateFormatter safeDateFormatter;

	/**
	 * @author pranab
	 *
	 */
	private static class SafeDateFormatter extends ThreadLocal<SimpleDateFormat> implements Serializable {
		private String dateFormatStr;
		
		/**
		 * @param dateFormatStr
		 */
		public SafeDateFormatter(String dateFormatStr) {
			this.dateFormatStr = dateFormatStr;
		}
		
		/* (non-Javadoc)
		 * @see java.lang.ThreadLocal#get()
		 */
		@Override
		public SimpleDateFormat get() {
		   return super.get();
		}

		/* (non-Javadoc)
		 * @see java.lang.ThreadLocal#initialValue()
		 */
		@Override
		protected SimpleDateFormat initialValue() {
		   return new SimpleDateFormat(dateFormatStr);
		}

		/* (non-Javadoc)
		 * @see java.lang.ThreadLocal#remove()
		 */
		@Override
		public void remove() {
		   super.remove();
		}

		/* (non-Javadoc)
		 * @see java.lang.ThreadLocal#set(java.lang.Object)
		 */
		@Override
		public void set(SimpleDateFormat value) {
		   super.set(value);
		}

	};	
	
	/**
	 * @param prAttr
	 */
	public DateValidatorHelper(ProcessorAttribute prAttr) {
		super();
		String datePattern = prAttr.getDatePattern();
		if (datePattern.equals(BasicUtils.EPOCH_TIME)) {
			epochTimeMs = true;
		} else if (datePattern.equals(BasicUtils.EPOCH_TIME_SEC)) {
			epochTimeMs = false;
		} else {
			safeDateFormatter = new SafeDateFormatter(prAttr.getDatePattern());
		}
	}

	/**
	 * @return
	 */
	public SimpleDateFormat getDateFormatter() {
		return safeDateFormatter.get();
	}

	/**
	 * @param dateFormatter
	 */
	public void setDateFormatter(SimpleDateFormat dateFormatter) {
		safeDateFormatter.set(dateFormatter);
	}

	/**
	 * @return
	 */
	public boolean isEpochTimeMs() {
		return epochTimeMs;
	}

	public void setEpochTimeMs(boolean epochTimeMs) {
		this.epochTimeMs = epochTimeMs;
	}

}
