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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.usc.ir.geo.gazetteer.service.Launcher;
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
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class GeoNameResolver implements Closeable {
	/**
	 * Below constants define name of field in lucene index
	 */
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
	/**
	 * Below constants define weight multipliers used for result relevance.
	 */
	private static final int WEIGHT_SORT_ORDER = 20;
	private static final int WEIGHT_SIZE_ALT_NAME = 50;
	private static final int WEIGHT_NAME_MATCH = 15000;

	private static final Logger LOG = Logger.getLogger(GeoNameResolver.class
			.getName());
	private static final Double OUT_OF_BOUNDS = 999999.0;
	private static Analyzer analyzer = new StandardAnalyzer();
	private static IndexWriter indexWriter;
	private static Directory indexDir;
	private static int hitsPerPage = 8;

	private IndexReader indexReader;

	@Option(name = "-b", aliases = {"--build"}, forbids = {"-s", "-server"},
			usage = "The Path to the Geonames allCountries.txt")
	private File gazetteerFile;

	@Option(name = "-i", aliases = {"--index"},
			usage = "The path to the Lucene index directory to either create or read")
	private File indexDirectory;

	@Option(name = "-s", aliases = {"--search"}, forbids = {"-b", "-server"},
			usage = "Location names to search the Gazetteer for")
	private List<String> geoNames;

	@Option(name = "-c", aliases = {"--count"}, depends = "-s",
			usage = "Number of best results to be returned for one location")
	private int count = 1;

	@Option(name = "-server", aliases = {"--server"},
			usage = "Launches Geo Gazetteer Service")
	private boolean launchServer;

	@Option(name="-p", aliases = {"--port"}, depends = "-server",
			usage = "Port number for launching service")
	private int port = 8765;

	@Option(name="-h", aliases = {"--help"},
			usage = "Show help message")
	private boolean showHelp;


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
	public HashMap<String, List<String>> searchGeoName(List<String> locationNames,
													   int count) throws IOException {
		if (this.indexReader == null) {
			this.indexReader = createIndexReader(this.indexDirectory.getPath());
		}
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

	public HashMap<String, List<String>> searchGeoName(String indexerPath,
													   List<String> locationNameEntities,
													   int count) throws IOException {

		if (locationNameEntities.size() == 0
				|| locationNameEntities.get(0).length() == 0)
			return new HashMap<String, List<String>>();
		IndexReader reader = createIndexReader(indexerPath);
		HashMap<String, List<String>> resolvedEntities =
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

	private HashMap<String, List<String>> resolveEntities(List<String> locationNames,
														  int count, IndexReader reader) throws IOException {
		if (locationNames.size() >= 200)
			hitsPerPage = 5; // avoid heavy computation
		IndexSearcher searcher = new IndexSearcher(reader);
		Query q = null;

		HashMap<String, List<List<String>>> allCandidates = new HashMap<String, List<List<String>>>();

		for (String name : locationNames) {

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
							tmp1.add(d.get(FIELD_NAME_COUNTRY_CODE));
							tmp1.add(d.get(FIELD_NAME_ADMIN1_CODE));
							tmp1.add(d.get(FIELD_NAME_ADMIN2_CODE));

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
		pickBestCandidates(resolvedEntities, allCandidates, count);
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
	 * @param count
	 * 			  Number of results for one locations
	 * @throws IOException
	 * @throws RuntimeException
	 */

	private void pickBestCandidates(
			HashMap<String, List<String>> resolvedEntities,
			HashMap<String, List<List<String>>> allCandidates, int count) {

		for (String extractedName : allCandidates.keySet()) {

			List<List<String>> cur = allCandidates.get(extractedName);
			if(cur.isEmpty())
				continue;//continue if no results found

			int maxWeight = Integer.MIN_VALUE ;
			//In case weight is equal for all return top element
			int bestIndex = 0;
			//Priority queue to return top elements
			PriorityQueue<List<String>> pq = new PriorityQueue<>(cur.size(), new Comparator<List<String>>() {
				@Override
				public int compare(List<String> o1, List<String> o2) {
					return Integer.compare(Integer.parseInt(o2.get(7)), Integer.parseInt(o1.get(7)));
				}
			});

			for (int i = 0; i < cur.size(); ++i) {
				int weight;
				// get cur's ith resolved entry's name
				String resolvedName = cur.get(i).get(0);
				//Assign a weight as per configuration if extracted name is found in name
				weight = resolvedName.contains(extractedName) ? WEIGHT_NAME_MATCH : 0;

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
				weight += (cur.size()-i) * WEIGHT_SORT_ORDER;

				cur.get(i).add(Integer.toString(weight));

				if (weight > maxWeight) {
					maxWeight = weight;
					bestIndex = i;
				}

				pq.add(cur.get(i)) ;
			}
			if (bestIndex == -1)
				continue;

			List<String> resultList = new ArrayList<>();

			for(int i =0 ; i< count && !pq.isEmpty() ; i++){
				List<String> result = pq.poll();
				//remove weight from allCandidates element before adding
				result.remove(7);
				//remove alternate name from allCandidates element before adding
				result.remove(3);

				resultList.addAll(result);
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
	 * @throws IOException
	 * @throws RuntimeException
	 */
	public void buildIndex()
			throws IOException {
		File indexfile = indexDirectory;
		indexDir = FSDirectory.open(indexfile.toPath());
		if (!DirectoryReader.indexExists(indexDir)) {
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			indexWriter = new IndexWriter(indexDir, config);
			Logger logger = Logger.getLogger(this.getClass().getName());
			logger.log(Level.WARNING, "Start Building Index for Gazatteer");
			BufferedReader filereader = new BufferedReader(
					new InputStreamReader(new FileInputStream(gazetteerFile),
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
		doc.add(new BinaryDocValuesField(FIELD_NAME_FEATURE_CLASS, new BytesRef(featureClass.getBytes())) );//sort enabled field
		doc.add(new BinaryDocValuesField(FIELD_NAME_FEATURE_CODE, new BytesRef(featureCode.getBytes())) );//sort enabled field
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
	 * Writes the result to given PrintStream
	 * @param resolvedEntities map of resolved entities
	 * @param out the print stream for writing output
	 */
	public static void writeResult(Map<String, List<String>> resolvedEntities,
								   PrintStream out) {
		out.println("[");
		List<String> keys = (List<String>)(List<?>) Arrays.asList(resolvedEntities.keySet().toArray());
		//TODO: use org.json.JSONArray and remove this custom formatting code
		for (int j=0; j < keys.size(); j++) {
			String n = keys.get(j);
			out.println("{\"" + n + "\" : [");
			List<String> terms = resolvedEntities.get(n);
			for (int i = 0; i < terms.size(); i++) {
				String res = terms.get(i);
				if (i < terms.size() - 1) {
					out.println("\"" + res + "\",");
				} else {
					out.println("\"" + res + "\"");
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
		GeoNameResolver resolver = new GeoNameResolver();
		CmdLineParser parser = new CmdLineParser(resolver);
		try {
			parser.parseArgument(args);
			if (resolver.showHelp) {
				System.out.println("lucene-geo-gazetteer \t");
				parser.printUsage(System.out);
				// Tika's geo parser uses this exit status to verify availability.
				//FIXME, Not all runtimes supports negative statuses
				System.exit(-1);
				return;
			}
		} catch (CmdLineException e){
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			System.exit(2);
		}
		if (resolver.indexDirectory != null && resolver.gazetteerFile != null) {
			LOG.info("Building Lucene index at path: [" + resolver.indexDirectory
					+ "] with geoNames.org file: [" + resolver.gazetteerFile + "]");
			resolver.buildIndex();
		}

		if (resolver.geoNames != null && !resolver.geoNames.isEmpty()) {
			Map<String, List<String>> resolved = resolver
					.searchGeoName(resolver.geoNames, resolver.count);
			writeResult(resolved, System.out);
		} else if (resolver.launchServer){
			Launcher.launchService(resolver.port,
					resolver.indexDirectory.getAbsolutePath());
		} else {
			System.err.println("Sub command not recognised");
			System.exit(3);
		}
	}

}
