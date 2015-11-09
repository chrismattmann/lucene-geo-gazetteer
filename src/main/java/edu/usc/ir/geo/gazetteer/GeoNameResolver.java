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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class GeoNameResolver {
	public static final String FIELD_NAME_ID = "ID";
	public static final String FIELD_NAME_NAME = "name";
	public static final String FIELD_NAME_LONGITUDE = "longitude";
	public static final String FIELD_NAME_LATITUDE = "latitude";
	public static final String FIELD_NAME_ALTERNATE_NAMES = "alternatenames";
	public static final String FIELD_NAME_FEATURE_CLASS = "featureClass";
	public static final String FIELD_NAME_FEATURE_CODE = "featureCode";
	public static final String FIELD_NAME_COUNTRY_CODE = "countryCode";
	public static final String FIELD_NAME_ADMIN1_CODE = "admin1Code";
	public static final String FIELD_NAME_ADMIN2_CODE = "admin2Code";
	public static final String FIELD_NAME_POPULATION = "population";
	
	private static final Logger LOG = Logger.getLogger(GeoNameResolver.class
			.getName());
	private static final Double OUT_OF_BOUNDS = 999999.0;
	private static Analyzer analyzer = new StandardAnalyzer();
	private static IndexWriter indexWriter;
	private static Directory indexDir;
	private static int hitsPerPage = 8;

	/**
	 * Search corresponding GeoName for each location entity
	 * 
	 * @param querystr
	 *            it's the NER actually
	 * @return HashMap each name has a list of resolved entities
	 * @throws IOException
	 * @throws RuntimeException
	 */

	public HashMap<String, List<String>> searchGeoName(String indexerPath, 
			List<String> locationNameEntities) throws IOException {

		if (locationNameEntities.size() == 0
				|| locationNameEntities.get(0).length() == 0)
			return new HashMap<String, List<String>>();
		File indexfile = new File(indexerPath);
		indexDir = FSDirectory.open(indexfile.toPath());


		if (!DirectoryReader.indexExists(indexDir)) {
			LOG.log(Level.SEVERE,
					"No Lucene Index Dierctory Found, Invoke indexBuild() First !");
			System.exit(1);
		}

		IndexReader reader = DirectoryReader.open(indexDir);

		if (locationNameEntities.size() >= 200)
			hitsPerPage = 5; // avoid heavy computation
		IndexSearcher searcher = new IndexSearcher(reader);
		
		Query q = null;

		HashMap<String, List<List<String>>> allCandidates = new HashMap<String, List<List<String>>>();

		for (String name : locationNameEntities) {

			if (!allCandidates.containsKey(name)) {
				try {
					//query is wrapped in additional quotes (") to avoid query tokenization on space
					q = new MultiFieldQueryParser(new String[] { FIELD_NAME_NAME,
							FIELD_NAME_ALTERNATE_NAMES }, analyzer).parse(String.format("\"%s\"", name) );
										
					//"feature class" sort order as defined in FeatureClassComparator
					SortField featureClassSort = CustomLuceneGeoGazetteerComparator.getFeatureClassSortField();
					
					//"feature code" sort order as defined in FeatureClassComparator
					SortField featureCodeSort = CustomLuceneGeoGazetteerComparator.getFeatureCodeSortField();
					
					//sort descending on population
					SortField populationSort = new SortedNumericSortField(FIELD_NAME_POPULATION, SortField.Type.LONG, true);
					
					Sort sort = new Sort(featureClassSort, featureCodeSort, populationSort);
					
					ScoreDoc[] hits = searcher.search(q, hitsPerPage , sort).scoreDocs;

					List<List<String>> topHits = new ArrayList<List<String>>();

					for (int i = 0; i < hits.length; ++i) {
						ArrayList<String> tmp1 = new ArrayList<String>();

						int docId = hits[i].doc;
						Document d;
						try {
							d = searcher.doc(docId);
							tmp1.add(d.get(FIELD_NAME_NAME));
							tmp1.add(d.get(FIELD_NAME_LONGITUDE));
							tmp1.add(d.get(FIELD_NAME_LATITUDE));
							//If alternate names are empty put name as actual name
							//This covers missing data and equals weight for later computation
							if (d.get(FIELD_NAME_ALTERNATE_NAMES).isEmpty()){
								tmp1.add(d.get(FIELD_NAME_NAME));
							}else{
								tmp1.add(d.get(FIELD_NAME_ALTERNATE_NAMES));
							}

						} catch (IOException e) {
							e.printStackTrace();
						}
						topHits.add(tmp1);
					}
					allCandidates.put(name, topHits);
				} catch (org.apache.lucene.queryparser.classic.ParseException e) {
					e.printStackTrace();
				}
			}
		}

		HashMap<String, List<String>> resolvedEntities = new HashMap<String, List<String>>();
		pickBestCandidates(resolvedEntities, allCandidates);
		reader.close();

		return resolvedEntities;

	}

	/**
	 * Select the best match for each location name extracted from a document,
	 * choosing from among a list of lists of candidate matches. Filter uses the
	 * following features: 1) edit distance between name and the resolved name,
	 * choose smallest one 2) content (haven't implemented)
	 * 
	 * @param resolvedEntities
	 *            final result for the input stream
	 * @param allCandidates
	 *            each location name may hits several documents, this is the
	 *            collection for all hitted documents
	 * @throws IOException
	 * @throws RuntimeException
	 */

	private void pickBestCandidates(
			HashMap<String, List<String>> resolvedEntities,
			HashMap<String, List<List<String>>> allCandidates) {

		for (String extractedName : allCandidates.keySet()) {
			
			List<List<String>> cur = allCandidates.get(extractedName);
			if(cur.isEmpty())
				continue;//continue if no results found
			
			int maxWeight = Integer.MIN_VALUE ;
			//In case weight is equal for all return top element
			int bestIndex = 0;
			for (int i = 0; i < cur.size(); ++i) {
				int weight;
				// get cur's ith resolved entry's name
				String resolvedName = cur.get(i).get(0);
				//Assign a weight of 10 if extracted name is found in name
				weight = resolvedName.contains(extractedName) ? 500 : 0;  
				
				// get all alternate names of cur's ith resolved entry's 
				String[] altNames = cur.get(i).get(3).split(",");
				float altEditDist = 0;
				for(String altName : altNames){
					if(altName.contains(extractedName)){
						altEditDist+=StringUtils.getLevenshteinDistance(extractedName, altName);
					}
				}
				//lesser the edit distance more should be the weight
				weight += getCalibratedWeight(altNames.length, altEditDist);
				
				//Give preference to sorted results. 0th result should have more priority
				weight += (cur.size()-i) * 2;
						
				if (weight > maxWeight) {
					maxWeight = weight;
					bestIndex = i;
				}
			}
			if (bestIndex == -1)
				continue;
			
			//remove alternate name from allCandidates element before adding
			cur.get(bestIndex).remove(3);
			resolvedEntities.put(extractedName, cur.get(bestIndex));
		}
	}

	/**
	 * Returns a weight for average edit distance for set of alternate name<br/><br/>
	 * altNamesSize * 50 - (altEditDist/altNamesSize) ;<br/><br/>
	 * altNamesSize * 50 ensure more priority for results with more alternate names.<br/> 
	 * altEditDist/altNamesSize is average edit distance. <br/>
	 * Lesser the average, higher the over all expression
	 * @param altNamesSize - Count of altNames
	 * @param altEditDist - sum of individual edit distances
	 * @return
	 */
	public float getCalibratedWeight(int altNamesSize, float altEditDist) { 
		return altNamesSize * 50 - (altEditDist/altNamesSize) ;
	}

	/**
	 * Build the gazetteer index line by line
	 * 
	 * @param gazetteerPath
	 *            path of the gazetteer file
	 * @param indexerPath
	 *            path to the created Lucene index directory.
	 * @throws IOException
	 * @throws RuntimeException
	 */
	public void buildIndex(String gazetteerPath, String indexerPath)
			throws IOException {
		File indexfile = new File(indexerPath);
		indexDir = FSDirectory.open(indexfile.toPath());
		if (!DirectoryReader.indexExists(indexDir)) {
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			indexWriter = new IndexWriter(indexDir, config);
			Logger logger = Logger.getLogger(this.getClass().getName());
			logger.log(Level.WARNING, "Start Building Index for Gazatteer");
			BufferedReader filereader = new BufferedReader(
					new InputStreamReader(new FileInputStream(gazetteerPath),
							"UTF-8"));
			String line;
			int count = 0;
			while ((line = filereader.readLine()) != null) {
				try {
					count += 1;
					if (count % 100000 == 0) {
						logger.log(Level.INFO, "Indexed Row Count: " + count);
					}
					addDoc(indexWriter, line);

				} catch (RuntimeException re) {
					logger.log(Level.WARNING, "Skipping... Error on line: {}",
							line);
				}
			}
			logger.log(Level.WARNING, "Building Finished");
			filereader.close();
			indexWriter.close();
		}
	}

	/**
	 * Index gazetteer's one line data by built-in Lucene Index functions
	 * 
	 * @param indexWriter
	 *            Lucene indexWriter to be loaded
	 * @param line
	 *            a line from the gazetteer file
	 * @throws IOException
	 * @throws NumberFormatException
	 */
	private static void addDoc(IndexWriter indexWriter, final String line) {
		String[] tokens = line.split("\t");

		int ID = Integer.parseInt(tokens[0]);
		String name = tokens[1];
		String alternatenames = tokens[3];

		Double latitude = -999999.0;
		try {
			latitude = Double.parseDouble(tokens[4]);
		} catch (NumberFormatException e) {
			latitude = OUT_OF_BOUNDS;
		}
		Double longitude = -999999.0;
		try {
			longitude = Double.parseDouble(tokens[5]);
		} catch (NumberFormatException e) {
			longitude = OUT_OF_BOUNDS;
		}

		int population = 0;
		try {
			population = Integer.parseInt(tokens[14]);
		} catch (NumberFormatException e) {
			population = 0;// Treat as population does not exists
		}

		// Additional fields to rank more known locations higher
		// All available codes can be viewed on www.geonames.org
		String featureClass = tokens[6];// broad category of location
		String featureCode = tokens[7];// more granular category
		String countryCode = tokens[8];
		String admin1Code = tokens[10];// eg US State
		String admin2Code = tokens[11];// eg county

		Document doc = new Document();
		doc.add(new IntField(FIELD_NAME_ID, ID, Field.Store.YES));
		doc.add(new TextField(FIELD_NAME_NAME, name, Field.Store.YES));
		doc.add(new DoubleField(FIELD_NAME_LONGITUDE, longitude, Field.Store.YES));
		doc.add(new DoubleField(FIELD_NAME_LATITUDE, latitude, Field.Store.YES));
		doc.add(new TextField(FIELD_NAME_ALTERNATE_NAMES, alternatenames, Field.Store.YES));
		//doc.add(new TextField(FIELD_NAME_FEATURE_CLASS, featureClass, Field.Store.YES));
		doc.add(new BinaryDocValuesField(FIELD_NAME_FEATURE_CLASS, new BytesRef(featureClass.getBytes())) );//sort enabled field
		//doc.add(new TextField(FIELD_NAME_FEATURE_CODE, featureCode, Field.Store.YES));
		doc.add(new BinaryDocValuesField(FIELD_NAME_FEATURE_CODE, new BytesRef(featureCode.getBytes())) );//sort enabled field
		doc.add(new TextField(FIELD_NAME_COUNTRY_CODE, countryCode, Field.Store.YES));
		doc.add(new TextField(FIELD_NAME_ADMIN1_CODE, admin1Code, Field.Store.YES));
		doc.add(new TextField(FIELD_NAME_ADMIN2_CODE, admin2Code, Field.Store.YES));
		//doc.add(new IntField(FIELD_NAME_POPULATION, population, Field.Store.YES));
		doc.add(new NumericDocValuesField(FIELD_NAME_POPULATION, population));//sort enabled field


		try {
			indexWriter.addDocument(doc);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws IOException {
		Option buildOpt = OptionBuilder.withArgName("gazetteer file").hasArg().withLongOpt("build")
				.withDescription("The Path to the Geonames allCountries.txt")
				.create('b');

		Option searchOpt = OptionBuilder.withArgName("set of location names").withLongOpt("search").hasArgs()
				.withDescription("Location names to search the Gazetteer for")
				.create('s');

		Option indexOpt = OptionBuilder
				.withArgName("directoryPath")
				.withLongOpt("index")
				.hasArgs()
				.withDescription(
						"The path to the Lucene index directory to either create or read")
				.create('i');

		Option helpOpt = OptionBuilder.withLongOpt("help")
				.withDescription("Print this message.").create('h');

		String indexPath = null;
		String gazetteerPath = null;
		List<String> geoTerms = null;
		Options options = new Options();
		options.addOption(buildOpt);
		options.addOption(searchOpt);
		options.addOption(indexOpt);
		options.addOption(helpOpt);

		// create the parser
		CommandLineParser parser = new DefaultParser();
		GeoNameResolver resolver = new GeoNameResolver();

		try {
			// parse the command line arguments
			CommandLine line = parser.parse(options, args);

			if (line.hasOption("index")) {
				indexPath = line.getOptionValue("index");
			}

			if (line.hasOption("build")) {
				gazetteerPath = line.getOptionValue("build");
			}

			if (line.hasOption("help")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("lucene-geo-gazetteer", options);
				System.exit(1);
			}

			if (indexPath != null && gazetteerPath != null) {
				LOG.info("Building Lucene index at path: [" + indexPath
						+ "] with geoNames.org file: [" + gazetteerPath + "]");
				resolver.buildIndex(gazetteerPath, indexPath);
			}

			if (line.hasOption("search")) {
				geoTerms = new ArrayList<String>(Arrays.asList(line
						.getOptionValues("search")));
				Map<String, List<String>> resolved = resolver
						.searchGeoName(indexPath, geoTerms);
				System.out.println("[");
				List<String> keys = (List<String>)(List<?>)Arrays.asList(resolved.keySet().toArray());
				for (int j=0; j < keys.size(); j++) {
					String n = keys.get(j);
					System.out.println("{\"" + n + "\" : [");
					List<String> terms = resolved.get(n);
					for (int i = 0; i < terms.size(); i++) {
						String res = terms.get(i);
						if (i < terms.size() - 1) {
							System.out.println("\"" + res + "\",");
						} else {
							System.out.println("\"" + res + "\"");
						}
					}
					
					if (j < keys.size() -1){
						System.out.println("]},");						
					}
					else{
						System.out.println("]}");
					}
				}
				System.out.println("]");
			}

		} catch (ParseException exp) {
			// oops, something went wrong
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
		}
	}

}
