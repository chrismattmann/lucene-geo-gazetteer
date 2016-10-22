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

package edu.usc.ir.geo.gazetteer;

import java.io.IOException;

public class RunReverseSearch {

	/**
	 * One sample program to run reverse search
	 */
	public static void main(String[] args) throws IOException {
		String indexPath = "./geoIndex";
		GeoNameResolver resolver = new GeoNameResolver();

		resolver.buildIndex("./cities1000.txt", indexPath, false);
		System.out.println("Places Near Los Angeles");
		System.out.println(resolver.searchNearby(34.022157, -118.285152, 0.1,indexPath, 10));
		
		resolver.close();

	}

}
