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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Big list of tuple. Spills data to disk when in memory size threshold exceeds.
 * Allows sequential write and read
 * @author pranab
 *
 */
public class BigTupleList {
	private List<Tuple> tuples;
	private int maxInMemory;
	private String spillDirPath;
	private String spillFilePath;
	public enum Mode {
	    Read, 
	    Write 
	}
	private Mode mode;
	private int readCount;
	private int writeCount;
	private byte[] tupleFieldTypes;
	private DataOutputStream outStream;
	private int size;
	private DataInputStream inStream;
	
	/**
	 * @param maxInMemory
	 * @param spillDirPath
	 */
	public BigTupleList(int maxInMemory, String spillDirPath) {
		super();
		this.maxInMemory = maxInMemory;
		this.spillDirPath = spillDirPath;
	}

	/**
	 * @param mode
	 */
	public void open(Mode mode) {
		if (mode == Mode.Read) {
			readCount = 0;
		} else {
			tuples = new ArrayList<Tuple>();
			writeCount = 0;
			size = 0;
		}
	}

	/**
	 * closes streams and deletes spill file if necessary 
	 */
	public void close()  {
		close(true);
	}	
	
	/**
	 * closes streams deletes spill file if necessary 
	 */
	public void close(boolean done)  {
		if (mode == Mode.Read) {
			try {
				if (null != inStream) {
					inStream.close();
					inStream = null;
					
					//delete spill file
					if (done) {
						File file = new File(spillFilePath);
			    		if(!file.delete()){
							throw new RuntimeException("Failed to delete spill file after read");
			    		}						
					}
				}
			} catch (IOException ioe) {
				throw new RuntimeException("Failed to close spill file after read" +ioe);
			}			
		} else {
			size = writeCount;
			try {
				if (null != outStream) {
					outStream.flush();
					outStream.close();
					outStream = null;
				}
			} catch (IOException ioe) {
				throw new RuntimeException("Failed to close spil file after write" +ioe);
			}
		}
	}

	/**
	 * @param tuple
	 */
	public Tuple write(Tuple tuple) {
		Tuple retTuple = null;
		if (writeCount < maxInMemory) {
			tuples.add(tuple);
			if (++writeCount == maxInMemory) {
				//switch to spill
				prepareForSpilWritel(tuple);
			}
		} else {
			//write to disk
			try {
				for (int i = 0; i < tupleFieldTypes.length; ++i ) {
					if (tupleFieldTypes[i] == Tuple.STRING) {
						outStream.writeUTF(tuple.getString(i));
					} else if (tupleFieldTypes[i] == Tuple.INT) {
						outStream.writeInt(tuple.getInt(i));
					} else if (tupleFieldTypes[i] == Tuple.LONG) {
						outStream.writeLong(tuple.getInt(i));
					} else if (tupleFieldTypes[i] == Tuple.DOUBLE) {
						outStream.writeDouble(tuple.getDouble(i));
					}
				}			
				retTuple = tuple;
			} catch (IOException ioe) {
				throw new RuntimeException("Failed spilling data to spill file" + ioe);
			}
		}
		
		return retTuple;
	}
	
	/**
	 * @return
	 */
	public Tuple read() {
		Tuple tuple = null;
		if (readCount < maxInMemory) {
			//from memory
			if (readCount < tuples.size()) {
				tuple = tuples.get(readCount);
				if (++readCount == maxInMemory) {
					prepareForSpilReadl();
				}
			}
		} else if (readCount < size){
			//from spill
			try {
				tuple = new Tuple();
				for (int i = 0; i < tupleFieldTypes.length; ++i ) {
					if (tupleFieldTypes[i] == Tuple.STRING) {
						tuple.add(inStream.readUTF());
					} else if (tupleFieldTypes[i] == Tuple.INT) {
						tuple.add(inStream.readInt());
					} else if (tupleFieldTypes[i] == Tuple.LONG) {
						tuple.add(inStream.readLong());
					} else if (tupleFieldTypes[i] == Tuple.DOUBLE) {
						tuple.add(inStream.readDouble());
					}
				}			
				++readCount;
			} catch (IOException ioe) {
				throw new RuntimeException("Failed to read data from spilli spill file" + ioe);
			}
		}
		return tuple;
	}
	
	public int getSize() {
		return size;
	}

	/**
	 * @param tuple
	 */
	private void prepareForSpilWritel(Tuple tuple) {
		tupleFieldTypes = new byte[tuple.getSize()];
		for (int i = 0; i < tupleFieldTypes.length; ++i ) {
			Object obj = tuple.get(i);
			if (obj instanceof String) {
				tupleFieldTypes[i] = Tuple.STRING;
			} else if (obj instanceof Integer) {
				tupleFieldTypes[i] = Tuple.INT;
			} else if (obj instanceof Long) {
				tupleFieldTypes[i] = Tuple.LONG;
			} else if (obj instanceof Double) {
				tupleFieldTypes[i] = Tuple.DOUBLE;
			}
		}
		spillFilePath = spillDirPath + "/spill-" + System.currentTimeMillis();
		try {
			outStream = new DataOutputStream(new FileOutputStream(spillFilePath));
		} catch (FileNotFoundException fnf) {
			throw new RuntimeException("error creating spill file" + fnf);
		}
	}

	/**
	 * @param tuple
	 */
	private void prepareForSpilReadl() {
        try {
			inStream = new DataInputStream(new FileInputStream(spillFilePath));
		} catch (FileNotFoundException fnf) {
			throw new RuntimeException("Error opening spill file for read" + fnf);
		}
	}

}
