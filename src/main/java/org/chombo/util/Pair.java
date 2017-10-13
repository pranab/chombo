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

import java.io.Serializable;

/**
 * Generic Pair class
 * @author pranab
 *
 * @param <L>
 * @param <R>
 */
public class Pair<L,R>  implements Serializable {
	protected  L left;
	protected  R right;

	/**
	 * 
	 */
	public Pair() {
	}
	  
	/**
	 * @param left
	 * @param right
	 */
	public Pair(L left, R right) {
	  this.left = left;
	  this.right = right;
	}

	/**
	 * @param left
	 * @param right
	 */
	public void initialize(L left, R right) {
		this.left = left;
		this.right = right;
	}
	
	/**
	 * @return
	 */
	public L getLeft() {
		return left;
	}

	/**
	 * @param left
	 */
	public void setLeft(L left) {
		this.left = left;
	}

	/**
	 * @return
	 */
	public R getRight() {
		return right;
	}

	/**
	 * @param right
	 */
	public void setRight(R right) {
		this.right = right;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() { 
		return left.hashCode() ^ right.hashCode();
	}

	  @Override
	  public boolean equals(Object other) {
		  boolean isEqual = false;
		  if (null != other && other instanceof Pair) {
			    Pair pairOther = (Pair) other;
			    isEqual =  this.left.equals(pairOther.left) &&
			           this.right.equals(pairOther.right); 
		  }
		   return isEqual;
	  }

}
