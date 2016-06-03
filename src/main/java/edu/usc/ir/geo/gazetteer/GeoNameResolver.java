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
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.usc.ir.geo.gazetteer.domain.Location;
import edu.usc.ir.geo.gazetteer.service.Launcher;

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

import com.google.gson.Gson;

public class GeoNameResolver implements Closeable {
	private static final String JSON_OPT = "json";
	/**
	 * Below constants define name of field in lucene index
	 */
	public static final String FIELD_NAME_ID = "ID";
	public static final String FIELD_NAME_NAME = "name";
	public static final String FIELD_NAME_LONGITUDE = "longitude";
	public static final String FIELD_NAME_LATITUDE = "latitude";
	public static final String FIELD_NAME_ALTERNATE_NAMES = "alternatenames";
	public static final String FIELD_NAME_FEATURE_CODE = "featureCode";
	public static final String FIELD_NAME_COUNTRY_CODE = "countryCode";
	public static final String FIELD_NAME_ADMIN1_CODE = "admin1Code";
	public static final String FIELD_NAME_ADMIN2_CODE = "admin2Code";
	public static final String FIELD_NAME_POPULATION = "population";
	/**
	 * Below constants define weight multipliers used for result relevance.
	 */
	private static final int WEIGHT_SORT_ORDER = 20;
	private static final int WEIGHT_SIZE_ALT_NAME = 50;
	private static final int WEIGHT_NAME_MATCH = 20000;
	private static final int WEIGHT_NAME_PART_MATCH = 15000;

	private static final Logger LOG = Logger.getLogger(GeoNameResolver.class
			.getName());
	private static final Double OUT_OF_BOUNDS = 999999.0;
	private static Analyzer analyzer = new StandardAnalyzer();
	private static IndexWriter indexWriter;
	private static Directory indexDir;
	private static int hitsPerPage = 8;

	private IndexReader indexReader;

	public GeoNameResolver(){
	}

	/**
	 * Creates a GeoNameResolver for given path
	 * @param indexPath the path to lucene index
	 * @throws IOException
	 */
	public GeoNameResolver(String indexPath) throws IOException {
		this.indexReader = createIndexReader(indexPath);
	}

	/**
	 *
	 * @param locationNames List of location na,es
	 * @param count Number of results per location
	 * @return resolved Geo Names
	 * @throws IOException
	 */
	public HashMap<String, List<Location>> searchGeoName(List<String> locationNames,
													   int count) throws IOException {
		return resolveEntities(locationNames, count, this.indexReader);
	}

	/**
	 * Search corresponding GeoName for each location entity
	 * @param count
	 * 			  Number of results for one locations
	 * @param querystr
	 *            it's the NER actually
	 *
	 * @return HashMap each name has a list of resolved entities
	 * @throws IOException
	 * @throws RuntimeException
	 */

	public HashMap<String, List<Location>> searchGeoName(String indexerPath,
													   List<String> locationNameEntities,
													   int count) throws IOException {

		if (locationNameEntities.size() == 0
				|| locationNameEntities.get(0).length() == 0)
			return new HashMap<String, List<Location>>();
		IndexReader reader = createIndexReader(indexerPath);
		HashMap<String, List<Location>> resolvedEntities =
				resolveEntities(locationNameEntities, count, reader);
		reader.close();
		return resolvedEntities;

	}

	private IndexReader createIndexReader(String indexerPath) throws IOException {
		File indexfile = new File(indexerPath);
		indexDir = FSDirectory.open(indexfile.toPath());


		if (!DirectoryReader.indexExists(indexDir)) {
			LOG.log(Level.SEVERE,
					"No Lucene Index Dierctory Found, Invoke indexBuild() First !");
			System.exit(1);
		}

		return DirectoryReader.open(indexDir);
	}

	private HashMap<String, List<Location>> resolveEntities(List<String> locationNames,
														  int count, IndexReader reader) throws IOException {
		if (locationNames.size() >= 200)
			hitsPerPage = 5; // avoid heavy computation
		IndexSearcher searcher = new IndexSearcher(reader);
		Query q = null;

		HashMap<String, List<Location>> allCandidates = new HashMap<String, List<Location>>();

		for (String name : locationNames) {

			if (!allCandidates.containsKey(name)) {
				try {
					//query is wrapped in additional quotes (") to avoid query tokenization on space
					q = new MultiFieldQueryParser(new String[] { FIELD_NAME_NAME,
							FIELD_NAME_ALTERNATE_NAMES }, analyzer).parse(String.format("\"%s\"", name) );

					//sort descending on population
					SortField populationSort = new SortedNumericSortField(FIELD_NAME_POPULATION, SortField.Type.LONG, true);

					Sort sort = new Sort(populationSort);
					//Fetch 3 times desired values, these will be sorted on code and only desired number will be kept
					ScoreDoc[] hits = searcher.search(q, hitsPerPage * 3 , sort).scoreDocs;

					List<Location> topHits = new ArrayList<Location>();

					for (int i = 0; i < hits.length; ++i) {
						Location tmpLocObj = new Location();

						int docId = hits[i].doc;
						Document d;
						try {
							d = searcher.doc(docId);
							tmpLocObj.setName(d.get(FIELD_NAME_NAME));
							tmpLocObj.setLongitude(d.get(FIELD_NAME_LONGITUDE));
							tmpLocObj.setLatitude(d.get(FIELD_NAME_LATITUDE));
							//If alternate names are empty put name as actual name
							//This covers missing data and equals weight for later computation
							if (d.get(FIELD_NAME_ALTERNATE_NAMES).isEmpty()){
								tmpLocObj.setAlternateNames(d.get(FIELD_NAME_NAME));
							}else{
								tmpLocObj.setAlternateNames(d.get(FIELD_NAME_ALTERNATE_NAMES));
							}
							tmpLocObj.setCountryCode(d.get(FIELD_NAME_COUNTRY_CODE));
							tmpLocObj.setAdmin1Code(d.get(FIELD_NAME_ADMIN1_CODE));
							tmpLocObj.setAdmin2Code(d.get(FIELD_NAME_ADMIN2_CODE));
							tmpLocObj.setFeatureCode(d.get(FIELD_NAME_FEATURE_CODE));

						} catch (IOException e) {
							e.printStackTrace();
						}
						topHits.add(tmpLocObj);
					}
					//Picking hitsPerPage number of locations from feature code sorted list 
					allCandidates.put(name, pickTopSortedByCode(topHits,hitsPerPage));
				} catch (org.apache.lucene.queryparser.classic.ParseException e) {
					e.printStackTrace();
				}
			}
		}

		HashMap<String, List<Location>> resolvedEntities = new HashMap<String, List<Location>>();
		pickBestCandidates(resolvedEntities, allCandidates, count);
		return resolvedEntities;
	}
	
	/**
	 * Sorts inputLocations as per FeatureCodeComparator and returns at most topCount locations 
	 * @param inputLocations List of locations to be sorted
	 * @param topCount Number of locations to be kept in curtailed list
	 * @return List of at most topCount locations sorted by edu.usc.ir.geo.gazetteer.CustomLuceneGeoGazetteerComparator.FeatureCodeComparator 
	 */
	private List<Location> pickTopSortedByCode(List<Location> inputLocations, int topCount) {
		if(inputLocations == null || inputLocations.size()==0){
			return new ArrayList<>();
		}
		
		Collections.sort(inputLocations, new CustomLuceneGeoGazetteerComparator.FeatureCodeComparator());
		return inputLocations.subList(0, inputLocations.size() > topCount ? topCount : inputLocations.size() - 1);
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
	 * @param count
	 * 			  Number of results for one locations
	 * @throws IOException
	 * @throws RuntimeException
	 */

	private void pickBestCandidates(
			HashMap<String, List<Location>> resolvedEntities,
			HashMap<String, List<Location>> allCandidates, int count) {

		for (String extractedName : allCandidates.keySet()) {

			List<Location> cur = allCandidates.get(extractedName);
			if(cur.isEmpty())
				continue;//continue if no results found

			int maxWeight = Integer.MIN_VALUE ;
			//In case weight is equal for all return top element
			int bestIndex = 0;
			//Priority queue to return top elements
			PriorityQueue<Location> pq = new PriorityQueue<>(cur.size(), new Comparator<Location>() {
				@Override
				public int compare(Location o1, Location o2) {
					return Integer.compare(o2.getWeight(), o1.getWeight());
				}
			});

			for (int i = 0; i < cur.size(); ++i) {
				int weight = 0;
				// get cur's ith resolved entry's name
				String resolvedName = String.format(" %s ", cur.get(i).getName());
				if (resolvedName.contains(String.format(" %s ", extractedName))) {
					// Assign a weight as per configuration if extracted name is found as a exact word in name
					weight = WEIGHT_NAME_MATCH;
				} else if (resolvedName.contains(extractedName)) {
					// Assign a weight as per configuration if extracted name is found partly in name
					weight = WEIGHT_NAME_PART_MATCH;
				}
				// get all alternate names of cur's ith resolved entry's
				String[] altNames = cur.get(i).getAlternateNames().split(",");
				float altEditDist = 0;
				for(String altName : altNames){
					if(altName.contains(extractedName)){
						altEditDist+=StringUtils.getLevenshteinDistance(extractedName, altName);
					}
				}
				//lesser the edit distance more should be the weight
				weight += getCalibratedWeight(altNames.length, altEditDist);

				//Give preference to sorted results. 0th result should have more priority
				weight += (cur.size()-i) * WEIGHT_SORT_ORDER;

				cur.get(i).setWeight(weight);

				if (weight > maxWeight) {
					maxWeight = weight;
					bestIndex = i;
				}

				pq.add(cur.get(i)) ;
			}
			if (bestIndex == -1)
				continue;

			List<Location> resultList = new ArrayList<>();

			for(int i =0 ; i< count && !pq.isEmpty() ; i++){
				resultList.add(pq.poll());
			}

			resolvedEntities.put(extractedName, resultList);
		}
	}

	/**
	 * Returns a weight for average edit distance for set of alternate name<br/><br/>
	 * altNamesSize * WEIGHT_SIZE_ALT_NAME - (altEditDist/altNamesSize) ;<br/><br/>
	 * altNamesSize * WEIGHT_SIZE_ALT_NAME ensure more priority for results with more alternate names.<br/>
	 * altEditDist/altNamesSize is average edit distance. <br/>
	 * Lesser the average, higher the over all expression
	 * @param altNamesSize - Count of altNames
	 * @param altEditDist - sum of individual edit distances
	 * @return
	 */
	public float getCalibratedWeight(int altNamesSize, float altEditDist) {
		return altNamesSize * WEIGHT_SIZE_ALT_NAME - (altEditDist/altNamesSize) ;
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
		doc.add(new TextField(FIELD_NAME_FEATURE_CODE, featureCode, Field.Store.YES));
		doc.add(new TextField(FIELD_NAME_COUNTRY_CODE, countryCode, Field.Store.YES));
		doc.add(new TextField(FIELD_NAME_ADMIN1_CODE, admin1Code, Field.Store.YES));
		doc.add(new TextField(FIELD_NAME_ADMIN2_CODE, admin2Code, Field.Store.YES));
		doc.add(new NumericDocValuesField(FIELD_NAME_POPULATION, population));//sort enabled field


		try {
			indexWriter.addDocument(doc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException {
		if (indexReader != null) {
			this.indexReader.close();
		}
	}
	/**
	 * Writes the result as formatted json to given PrintStream 
	 * @param resolvedEntities map of resolved entities
	 * @param out the print stream for writing output
	 */
	public static void writeResultJson(Map<String, List<Location>> resolvedEntities,
			   PrintStream out) {
		out.println(new Gson().toJson(resolvedEntities) );
	}
	
	/**
	 * Writes the result to given PrintStream
	 * @deprecated Use writeResultJson instead 
	 * @param resolvedEntities map of resolved entities
	 * @param out the print stream for writing output
	 */
	@Deprecated
	public static void writeResult(Map<String, List<Location>> resolvedEntities,
								   PrintStream out) {
		out.println("[");
		List<String> keys = (List<String>)(List<?>) Arrays.asList(resolvedEntities.keySet().toArray());
		//TODO: use org.json.JSONArray and remove this custom formatting code
		for (int j=0; j < keys.size(); j++) {
			String n = keys.get(j);
			out.println("{\"" + n + "\" : [");
			List<Location> terms = resolvedEntities.get(n);
			for (int i = 0; i < terms.size(); i++) {
				Location res = terms.get(i);
				if (i < terms.size() - 1) {
					out.println(res + ",");
				} else {
					out.println(res);
				}
			}

			if (j < keys.size() -1){
				out.println("]},");
			}
			else{
				out.println("]}");
			}
		}
		out.println("]");
	}

	public static void main(String[] args) throws Exception {
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

		Option resultCountOpt = OptionBuilder.withArgName("number of results").withLongOpt("count").hasArgs()
				.withDescription("Number of best results to be returned for one location").withType(Integer.class)
				.create('c');

		Option serverOption = OptionBuilder.withArgName("Launch Server")
				.withLongOpt("server")
				.withDescription("Launches Geo Gazetteer Service")
				.create("server");

		Option jsonOption = OptionBuilder.withArgName("outputs json")
				.withLongOpt(JSON_OPT)
				.withDescription("Formats output in well defined json structure")
				.create(JSON_OPT);

		String indexPath = null;
		String gazetteerPath = null;
		Options options = new Options();
		options.addOption(buildOpt);
		options.addOption(searchOpt);
		options.addOption(indexOpt);
		options.addOption(helpOpt);
		options.addOption(resultCountOpt);
		options.addOption(serverOption);
		options.addOption(jsonOption);

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
				List<String> geoTerms = new ArrayList<String>(Arrays.asList(line
						.getOptionValues("search")));
				String countStr = line.getOptionValue("count", "1");
				int count = 1;
				if (countStr.matches("\\d+"))
					count = Integer.parseInt(countStr);

				Map<String, List<Location>> resolved = resolver
						.searchGeoName(indexPath, geoTerms, count);
				if(line.hasOption(JSON_OPT)){
					writeResultJson(resolved, System.out);
				}else{
					writeResult(resolved, System.out);
				}
			} else if (line.hasOption("server")){
				if (indexPath == null) {
					System.err.println("Index path is required");
					System.exit(-2);
				}

				//TODO: get port from CLI args
				int port = 8765;
				Launcher.launchService(port, indexPath);
			}else if (!line.hasOption("server") &&
				!line.hasOption("search") &&
				!line.hasOption("build") &&
				!line.hasOption("index") &&
				!line.hasOption("help")) 
			{
				System.err.println("Sub command not recognised");
				System.exit(-1);
			}

		} catch (ParseException exp) {
			// oops, something went wrong
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
		}
	}

}
