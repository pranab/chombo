package org.chombo.util;

/**
 * @author pranab
 *
 */
public class DoubleRange extends Pair<Double, Double>{
	private boolean isPoint; 
	private String delim;
	private int outputPrecision;
	
	/**
	 * @param stVal
	 * @param delim
	 */
	public DoubleRange(String stVal, String delim, int outputPrecision) {
		String[] items = stVal.split(delim);
		if (items.length == 1) {
			double val = Double.parseDouble(stVal);
			initialize(val, val);
			isPoint = true;
		} else {
			left = Double.parseDouble(items[0]);
			right = Double.parseDouble(items[1]);
		}
		this.delim = delim;
		this.outputPrecision = outputPrecision;
	}
	
	/**
	 * @param value
	 */
	public void add(double value) {
		if (modify(value) && isPoint) {
			isPoint = false;
		}
	}
	
	/**
	 * @param value
	 * @return
	 */
	private boolean modify(double value) {
		boolean modified = true;
		if (value < left) {
			left = value;
		} else if (value > right) {
			right = value;
		} else {
			modified = false;
		}
		return modified;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return BasicUtils.formatDouble(left, outputPrecision) + delim + 
				BasicUtils.formatDouble(right, outputPrecision);
	}
	
}
