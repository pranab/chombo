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
 * @author pranab
 *
 */
public class SpatioTemporalPoint {
	private double latitude;
	private double longitude;
	private long time;
	
	public SpatioTemporalPoint(double latitude, double longitude, long time) {
		super();
		this.latitude = latitude;
		this.longitude = longitude;
		this.time = time;
	}
	
	public double getLatitude() {
		return latitude;
	}
	
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	
	public double getLongitude() {
		return longitude;
	}
	
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	
	public long getTime() {
		return time;
	}
	
	public void setTime(long time) {
		this.time = time;
	}
	
	public void timeToHour(int numHours) {
		time /= (BasicUtils.MILISEC_PER_HOUR * numHours);
	}
	
	public double geoDistance(SpatioTemporalPoint other) {
		return BasicUtils.getGeoDistance(latitude, longitude, other.latitude, other.longitude);
	}
	
	public long timeDifference(SpatioTemporalPoint other) {
		return Math.abs(time - other.time);
	}
}
