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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * General purpose tuple consisting list of primitive types. Implements WritableComparable
 * @author pranab
 *
 */
public class Tuple  implements WritableComparable<Tuple>  {
	public static final byte BOOLEAN = 1;
	public static final byte INT = 2;
	public static final byte LONG = 3;
	public static final byte FLOAT = 4;
	public static final byte DOUBLE = 5;
	public static final byte STRING = 6;
	
	private List<Object> fields;
	
	public Tuple() {
		fields = new ArrayList<Object>();
	}
	
	public void initialize() {
		fields.clear();
	}
	
	public void add(Object field) {
		fields.add(field);
	}

	public void add(byte type, String field) {
		Object typedField = null;
		
		if (type ==  BOOLEAN ) {
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
		} else {
			throw new IllegalArgumentException("Failed adding element to tuple, unknown element type");
		}
		
		if (null != typedField){
			fields.add(typedField);
		}
	}

	public void set(int index, Object field) {
		fields.add(index, field);
	}
	
	public Object get(int index) {
		return fields.get(index);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int numFields = in.readInt();
		
		for(int i = 0;  i < numFields;  ++i) {
			byte type = in.readByte();
			
			if (type ==  BOOLEAN ) {
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
			} else {
				throw new IllegalArgumentException("Failed encoding, unknown element type in stream");
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(fields.size());
		
		for(Object field : fields) {
			if (field instanceof Boolean){
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
			} else {
				throw new IllegalArgumentException("Failed encoding, unknown element type in tuple");
			}
		}
	}

	public int hashCode() {
		return fields.hashCode();
	}
	
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
				if (field instanceof Boolean){
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
					throw new IllegalArgumentException("Failed in compare, unknown element type in tuple");
				}
			}
		} else {
			throw new IllegalArgumentException("Can not compare tuples of unequal length");
		}
		return compared;
	}
}
