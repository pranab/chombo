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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

/**
 * General purpose tuple consisting list of primitive types. Implements WritableComparable
 * @author pranab
 *
 */
public class Tuple  implements WritableComparable<Tuple> , Serializable {
	public static final byte BYTE = 0;
	public static final byte BOOLEAN = 1;
	public static final byte INT = 2;
	public static final byte LONG = 3;
	public static final byte FLOAT = 4;
	public static final byte DOUBLE = 5;
	public static final byte STRING = 6;
	public static final byte BYTE_ARRAY = 7;
	public static final byte TUPLE = 8;
	
	private List<Object> fields;
	private String delim = ",";
	
	/**
	 * 
	 */
	public Tuple() {
		fields = new ArrayList<Object>();
	}
	
	/**
	 * @param fields
	 */
	public Tuple(List<Object> fields) {
		this.fields = fields;
	}
	
	/**
	 * creates clone
	 * @return
	 */
	public Tuple createClone() {
		Tuple clone = new Tuple();
		clone.fields.addAll(fields);
		return clone;
	}

	/**
	 * @param clone
	 * @return
	 */
	public Tuple createClone(Tuple clone) {
		clone.initialize();
		clone.fields.addAll(fields);
		return clone;
	}
	
	/**
	 * clears internal list
	 */
	public void initialize() {
		fields.clear();
	}
	
	/**
	 * gets size
	 * @return
	 */
	public int getSize() {
		return fields.size();
	}
	
	/**
	 * add one or more elements
	 * @param fieldList
	 */
	public void add(Object...  fieldList) {
		for (Object field :  fieldList) {
			fields.add(field);
		}
	}

	/**
	 * prepends element
	 * @param field
	 */
	public void prepend(Object field) {
		fields.add(0, field);
	}

	/**
	 * appends element
	 * @param field
	 */
	public void append(Object field) {
		fields.add( field);
	}
	
	/**
	 * @param field
	 * @param index
	 */
	public void insert(Object field, int index) {
		fields.add(index, field);
	}

	/**
	 * @param types
	 * @param fields
	 */
	public void add(byte[] types, String[] fields) {
		for (int i = 0; i <  fields.length; ++i) {
			add(types[i],  fields[i]) ;
		}
	}
	
	/**
	 * @param other
	 */
	public void add(Tuple other) {
		fields.addAll(other.fields);
	}
	
	 /**
	 * @param list
	 */
	public  <T> void add(List<T> list) {
		 fields.addAll(list);
	 }
	 
	/**
	 * adds string serilized elements
	 * @param type
	 * @param field
	 */
	public void add(byte type, String field) {
		Object typedField = null;
		
		if (type ==  BYTE ) {
			typedField = Byte.decode(field);
		} else if (type ==  BOOLEAN ) {
			typedField = Boolean.parseBoolean(field);
		} else if (type ==  INT ) {
			typedField = Integer.parseInt(field);
		}  else if (type ==  LONG ) {
			typedField =  Long.parseLong(field);
		}  else if (type ==  FLOAT ) {
			typedField = Float.parseFloat(field);
		} else if (type ==  DOUBLE ) {
			typedField = Double.parseDouble(field);
		} else if (type ==  STRING) {
			typedField = field;
		} else if (type ==  BYTE_ARRAY) {
			try {
				typedField = field.getBytes("utf-8");
			} catch (UnsupportedEncodingException e) {
				throw new IllegalArgumentException("Failed adding element to tuple, unknown element type");
			}
		}  else {
			throw new IllegalArgumentException("Failed adding element to tuple, unknown element type");
		}
		
		if (null != typedField){
			fields.add(typedField);
		}
	}

	/**
	 * @param vec
	 */
	public void fromArray(double[] vec) {
		for (double val : vec) {
			add(val);
		}
	}
	
	/**
	 * @return
	 */
	public double[] toDoubleArray() {
		double[] vec = new double[fields.size()];
		for (int i = 0; i < vec.length; ++i) {
			vec[i] = getDouble(i);
		}
		return vec;
	}
	
	/**
	 * @param vec
	 */
	public void fromArray(int[] vec) {
		for (double val : vec) {
			add(val);
		}
	}
	
	/**
	 * @return
	 */
	public int[] toIntArray() {
		int[] vec = new int[fields.size()];
		for (int i = 0; i < vec.length; ++i) {
			vec[i] = getInt(i);
		}
		return vec;
	}

	/**
	 * @param items
	 * @param start
	 * @param end
	 */
	public <T> void addFromArray(T[] items) {
		addFromArray(items, 0, items.length);
	}

	/**
	 * @param items
	 * @param start
	 * @param end
	 */
	public <T> void addFromArray(T[] items, int start, int end) {
		for (int index = start;   index < end; ++index) {
			add(items[index]);
		}
	}
	
	/**
	 * Adds multiple contiguous elements of an array
	 * @param items
	 * @param indexes
	 */
	public <T> void addFromArray(T[] items, int[] indexes) {
		for (int index  :  indexes) {
			add(items[index]);
		}
	}
	
	/**
	 * Adds multiple elements of an array
	 * @param items
	 * @param indexes
	 */
	public  <T> void addArrayElements(T[] items, int[] indexes) {
    	if (null != indexes) {
    		for (int i  :  indexes) {
    			add(items[i]);
    		}
    	}
	}
	
	/**
	 * sets specific element
	 * @param index
	 * @param field
	 */
	public void set(int index, Object field) {
		fields.add(index, field);
	}
	
	/**
	 * gets specific element
	 * @param index
	 * @return
	 */
	public Object get(int index) {
		return fields.get(index);
	}
	
	/**
	 * gets string from specific index
	 * @param index
	 * @return
	 */
	public String getString(int index) {
		return (String)fields.get(index);
	}

	/**
	 * gets last element as string
	 * @return
	 */
	public String getLastAsString() {
		return (String)fields.get(fields.size()-1);
	}

	/**
	 * gets int from specific index
	 * @param index
	 * @return
	 */
	public int getInt(int index) {
		return (Integer)fields.get(index);
	}

	/**
	 * gets last element as int
	 * @return
	 */
	public int getLastAsInt() {
		return (Integer)fields.get(fields.size()-1);
	}

	/**
	 * gets long from specific index
	 * @param index
	 * @return
	 */
	public long getLong(int index) {
		return (Long)fields.get(index);
	}

	/**
	 * gets last element as long
	 * @return
	 */
	public long getLastAsLong() {
		return (Long)fields.get(fields.size()-1);
	}

	/**
	 * gets double from specific index
	 * @param index
	 * @return
	 */
	public double getDouble(int index) {
		return (Double)fields.get(index);
	}
	
	/**
	 * gets last element as double
	 * @return
	 */
	public double getLastAsDouble() {
		return (Double)fields.get(fields.size()-1);
	}

	/**
	 * return true if the element is int
	 * @param index
	 * @return
	 */
	public boolean isInt(int index) {
		Object obj = fields.get(index);
		return obj instanceof Integer;
	}

	/**
	 * return true if the element is string
	 * @param index
	 * @return
	 */
	public boolean isString(int index) {
		Object obj = fields.get(index);
		return obj instanceof String;
	}

	/**
	 * return true if the element is boolean
	 * @param index
	 * @return
	 */
	public boolean isDouble(int index) {
		Object obj = fields.get(index);
		return obj instanceof Double;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		initialize();
		int numFields = in.readInt();
		
		for(int i = 0;  i < numFields;  ++i) {
			byte type = in.readByte();
			
			if (type ==  BYTE ) {
				fields.add(in.readByte());
			} else if (type ==  BOOLEAN ) {
				fields.add(in.readBoolean());
			} else if (type ==  INT ) {
				fields.add(in.readInt());
			}  else if (type ==  LONG ) {
				fields.add(in.readLong());
			}  else if (type ==  FLOAT ) {
				fields.add(in.readFloat());
			} else if (type ==  DOUBLE ) {
				fields.add(in.readDouble());
			} else if (type ==  STRING) {
				fields.add(in.readUTF());
			} else if (type ==  BYTE_ARRAY) {
				int  len = in.readShort();
				byte[] bytes = new byte[len];
				in.readFully(bytes);
				fields.add(bytes);
			} else if (type ==  TUPLE) {
				Tuple childTuple = new Tuple(); 
				childTuple.readFields(in);
				fields.add(childTuple);
			} else {
				throw new IllegalArgumentException("Failed encoding, unknown element type in stream");
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(fields.size());
		
		for(Object field : fields) {
			if (field instanceof Byte){
				out.writeByte(BYTE);	
				out.writeByte((Byte)field);
			} else if (field instanceof Boolean){
				out.writeByte(BOOLEAN);	
				out.writeBoolean((Boolean)field);
			} else if (field instanceof Integer){
				out.writeByte(INT);	
				out.writeInt((Integer)field);
			} else if (field instanceof Long){
				out.writeByte(LONG);	
				out.writeLong((Long)field);
			} else if (field instanceof Float){
				out.writeByte(FLOAT);	
				out.writeFloat((Float)field);
			} else if (field instanceof Double){
				out.writeByte(DOUBLE);	
				out.writeDouble((Double)field);
			} else if (field instanceof String){
				out.writeByte(STRING);	
				out.writeUTF((String)field);
			} else if (field instanceof byte[]){
				byte[] bytes = (byte[])field;
				out.writeByte(BYTE_ARRAY);
				out.writeShort(bytes.length);
				out.write(bytes);
			} else if (field instanceof Tuple){
				out.writeByte(TUPLE);
				((Tuple)field).write(out);
			} else {
				throw new IllegalArgumentException("Failed encoding, unknown element type in tuple");
			}
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return fields.hashCode();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj ) {
		boolean isEqual = false;
		if (null != obj && obj instanceof Tuple){
			isEqual =  ((Tuple)obj).fields.equals(fields);
		}
		return isEqual;
	}

	@Override
	public int compareTo(Tuple that) {
		int compared = 0;
		if (fields.size() == that.fields.size()) {
			for(int i = 0; i <  fields.size() && compared == 0; ++i) {
				Object field = fields.get(i);
				if (field instanceof Byte){
					compared = ((Byte)field).compareTo((Byte)that.fields.get(i));	
				} else if (field instanceof Boolean){
					compared = ((Boolean)field).compareTo((Boolean)that.fields.get(i));	
				} else if (field instanceof Integer){
					compared = ((Integer)field).compareTo((Integer)that.fields.get(i));	
				} else if (field instanceof Long){
					compared = ((Long)field).compareTo((Long)that.fields.get(i));	
				} else if (field instanceof Float){
					compared = ((Float)field).compareTo((Float)that.fields.get(i));	
				} else if (field instanceof Double){
					compared = ((Double)field).compareTo((Double)that.fields.get(i));	
				} else if (field instanceof String){
					compared = ((String)field).compareTo((String)that.fields.get(i));	
				}  else {
					throw new IllegalArgumentException("Failed in compare, unknown element type in tuple  ");
				}
			}
		} else {
			throw new IllegalArgumentException("Can not compare tuples of unequal length this:"  + 
					fields.size() + " that:" +  that.fields.size());
		}
		return compared;
	}
	
	/**
	 * comparison based on all but the last element
	 * @param other
	 * @return
	 */
	public int compareToBase(Tuple other) {
		Tuple subThis = new Tuple(fields.subList(0,fields.size()-1));
		Tuple subThat = new Tuple(other.fields.subList(0,other.fields.size()-1));
		return subThis.compareTo(subThat);
	}
	
	/**
	 * hash code based on all but the last element
	 * @return
	 */
	public int hashCodeBase() {
		Tuple subThis = new Tuple(fields.subList(0,fields.size()-1));
		int hashCode =  subThis.hashCode();
		hashCode = hashCode < 0 ? -hashCode : hashCode;
		return hashCode;
	}
	
	/**
	 * hash based on partial list
	 * @param subLength
	 * @return
	 */
	public int hashCodePartial(int subLength) {
		Tuple subThis = new Tuple(fields.subList(0,subLength));
		return subThis.hashCode();
	}

	/**
	 * returns true if starts with given object
	 * @param obj
	 * @return
	 */
	public boolean startsWith(Object obj) {
		return obj.equals(fields.get(0));
	}
	
	/**
	 * sets delimeter
	 * @param delim
	 */
	public void setDelim(String delim) {
		this.delim = delim;
	}

	/**
	 * sets delimeter
	 * @param delim
	 */
	public Tuple withDelim(String delim) {
		this.delim = delim;
		return this;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuilder stBld = new  StringBuilder();
		for(int i = 0; i <  fields.size() ; ++i) {
			if (i == 0){
				stBld.append(fields.get(i).toString());
			} else {
				stBld.append(delim).append(fields.get(i).toString());
			}
		}		
		return stBld.toString();
	}
	
	/**
	 * to string starting at given index
	 * @param start
	 * @return
	 */
	public String toString(int start) {
		return toString(start, fields.size());
	}
	
	/**
	 * @param start
	 * @return
	 */
	public String toStringBeg(int start) {
		return toString(start, fields.size());
	}
	
	/**
	 * @param end
	 * @return
	 */
	public String toStringEnd(int end) {
		return toString(0, end);
	}

	/**
	 * to string starting at given index
	 * @param start
	 * @param end
	 * @return
	 */
	public String toString(int start, int end) {
		StringBuilder stBld = new  StringBuilder();
		for(int i = start; i <  end ; ++i) {
			if (i == start){
				stBld.append(fields.get(i).toString());
			} else {
				stBld.append(delim).append(fields.get(i).toString());
			}
		}		
		return stBld.toString();
	}

	/**
	 * @param offset
	 * @return
	 */
	public Tuple beginningSubTuple(int offset) {
		return subTuple(0, offset);
	}
	
	/**
	 * @param offset
	 * @return
	 */
	public Tuple endSubTuple(int offset) {
		return subTuple(offset, fields.size());
	}

	/**
	 * creates tuple based on partial list of source tuple
	 * @param start
	 * @param end
	 * @return
	 */
	public Tuple subTuple(int start, int end) {
		if (end < start) {
			throw new IllegalArgumentException("end index is smaller that start index");
		}
		
		Tuple subTuple = new Tuple();
		for (int i = start; i < end; ++i) {
			subTuple.add(get(i));
		}
		return subTuple;
	}
	
	/**
	 * creates tuple based on partial list of source tuple
	 * @param start
	 * @param end
	 * @return
	 */
	public String[]  subTupleAsArray(int start, int end) {
		if (end < start) {
			throw new IllegalArgumentException("end index is smaller that start index");
		}
		
		String[] subTuple = new String[end - start];
		for (int i = start; i < end; ++i) {
			subTuple[i - start] = get(i).toString();
		}
		return subTuple;
	}
	
	/**
	 * @param start
	 * @return
	 */
	public String[]  subTupleAsArray(int start) {
		return subTupleAsArray(start, fields.size());
	}
	
	/**
	 * @return
	 */
	public String[] getTupleAsArray() {
		return subTupleAsArray(0, fields.size());
	}

	/**
	 * @param start
	 * @param end
	 * @return
	 */
	public <T> void subTupleAsList(int start, int end, List<T> list) {
		for (int i = start; i < end; ++i) {
			list.add((T)get(i));
		}
	}
	
	/**
	 * @param start
	 * @return
	 */
	public <T> void subTupleAsList(int start, List<T> list) {
		subTupleAsList(start, fields.size(), list);
	}
	
	/**
	 * @param start
	 * @return
	 */
	public <T> void tupleAsList(List<T> list) {
		subTupleAsList(0, fields.size(), list);
	}

	/**
	 * removes duplicates and maintains same order
	 */
	public void removeDuplicates() {
		List<Object> uniqueFields = new ArrayList<Object>();
		for (Object value : fields) {
			if (!uniqueFields.contains(value)) {
				uniqueFields.add(value);
			}
		}
		fields = uniqueFields;
	}
	
}
