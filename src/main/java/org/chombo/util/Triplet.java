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
 * Triplet generic
 * @author pranab
 *
 * @param <L>
 * @param <C>
 * @param <R>
 */
public class Triplet<L, C, R> extends Pair<L,R> {
	private C center;

	public Triplet() {
		super();
	}

	public Triplet(L left, C center, R right) {
		super(left, right);
		this.center = center;
	}

	public C getCenter() {
		return center;
	}

	public void setCenter(C center) {
		this.center = center;
	}

	@Override
	public int hashCode() { 
		return left.hashCode() ^ center.hashCode() ^ right.hashCode();
	}

	  @Override
	  public boolean equals(Object other) {
		  boolean isEqual = false;
		  if (null != other && other instanceof Triplet) {
			  Triplet tripleOther = (Triplet) other;
			    isEqual =  this.left.equals(tripleOther.left) &&
			    		this.center.equals(tripleOther.center) &&
			    		this.right.equals(tripleOther.right); 
		  }
		   return isEqual;
	  }

}
