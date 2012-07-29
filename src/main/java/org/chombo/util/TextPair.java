package org.chombo.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author pranab
 *
 */
public class TextPair implements WritableComparable<TextPair> {
	private Text first;
	private Text second;
	private String delim = ",";
	
	public TextPair() {
		first = new Text();
		second =  new Text();
	}
	
	public void set(String first, String second) {
		this.first.set(first);
		this.second.set(second);
	}
	
	public Text getFirst() {
		return first;
	}
	public void setFirst(String first) {
		this.first .set(first);
	}
	public Text getSecond() {
		return second;
	}
	public void setSecond(String second) {
		this.second.set(second);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		
	}
	@Override
	public int compareTo(TextPair tePair) {
		int cmp = first.compareTo(tePair.getFirst());
		if (0 == cmp) {
			cmp = second.compareTo(tePair.getSecond());
		}
		return cmp;
	}

	public int baseCompareTo(TextPair other) {
		int cmp = first.compareTo(other.getFirst());
		return cmp;
	}
	
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
	
	public int baseHashCode() {
		return Math.abs(first.hashCode());
	}
	
	public boolean equals(Object obj) {
		boolean isEqual =  false;
		if (obj instanceof TextPair) {
			TextPair iPair = (TextPair)obj;
			isEqual = first.equals(iPair.first) && second.equals(iPair.second);
		}
		
		return isEqual;
	}
	
	public void setDelim(String delim) {
		this.delim = delim;
	}

	public String toString() {
		return "" + first + delim + second;
	}

}
