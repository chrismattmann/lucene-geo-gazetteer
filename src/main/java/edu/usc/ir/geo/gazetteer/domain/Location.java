/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.usc.ir.geo.gazetteer.domain;

public class Location {

	private String name;
	
	private transient String alternateNames;
	private transient String featureCode;
	private String countryCode;
	private String admin1Code;
	private String admin2Code;
	private double latitude;
	private double longitude;
	private transient int weight;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAlternateNames() {
		return alternateNames;
	}
	public void setAlternateNames(String alternateNames) {
		this.alternateNames = alternateNames;
	}
	public String getCountryCode() {
		return countryCode;
	}
	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}
	public String getAdmin1Code() {
		return admin1Code;
	}
	public void setAdmin1Code(String admin1Code) {
		this.admin1Code = admin1Code;
	}
	public String getAdmin2Code() {
		return admin2Code;
	}
	public void setAdmin2Code(String admin2Code) {
		this.admin2Code = admin2Code;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = Double.parseDouble(latitude);
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = Double.parseDouble(longitude);
	}
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}
	public String getFeatureCode() {
		return featureCode;
	}
	public void setFeatureCode(String featureCode) {
		this.featureCode = featureCode;
	}
	
	@Override
	public String toString() {
		return createCSV();
	}
	
	/**
	 * @return a csv string to support older version of API
	 */
	public String createCSV() {
		StringBuilder sb = new StringBuilder();
		sb.append("\""+ name + "\",");
		sb.append("\""+ longitude + "\",");
		sb.append("\""+ latitude + "\",");
		sb.append("\""+ countryCode + "\",");
		sb.append("\""+ admin1Code + "\",");
		sb.append("\""+ admin2Code + "\"");
		
		return sb.toString();
	}
	
	

}
