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

import java.text.SimpleDateFormat;

import org.chombo.util.BasicUtils;
import org.chombo.util.ProcessorAttribute;

/**
 * @author pranab
 *
 */
public class DateValidatorHelper {
	private SimpleDateFormat dateFormatter;
	private boolean epochTimeMs;

	public DateValidatorHelper(ProcessorAttribute prAttr) {
		super();
		String datePattern = prAttr.getDatePattern();
		if (datePattern.equals(BasicUtils.EPOCH_TIME)) {
			epochTimeMs = true;
		} else if (datePattern.equals(BasicUtils.EPOCH_TIME_SEC)) {
			epochTimeMs = false;
		} else {
			dateFormatter = new SimpleDateFormat(prAttr.getDatePattern());
		}
	}

	public SimpleDateFormat getDateFormatter() {
		return dateFormatter;
	}

	public void setDateFormatter(SimpleDateFormat dateFormatter) {
		this.dateFormatter = dateFormatter;
	}

	public boolean isEpochTimeMs() {
		return epochTimeMs;
	}

	public void setEpochTimeMs(boolean epochTimeMs) {
		this.epochTimeMs = epochTimeMs;
	}

}
