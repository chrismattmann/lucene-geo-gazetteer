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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

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
													   List<String> locationNameEntities, int count) throws IOException {

		if (locationNameEntities.size() == 0
				|| locationNameEntities.get(0).length() == 0)
			return new HashMap<String, List<String>>();
		IndexReader reader = createIndexReader(indexerPath);
		HashMap<String, List<String>> resolvedEntities = resolveEntities(locationNameEntities, count, reader);
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

	public static class SearchHandler extends AbstractHandler {
		public static final String Q_PARAM = "q";
		public static final String COUNT_PARAM = "count";

		private final GeoNameResolver resolver;

		public SearchHandler(GeoNameResolver resolver) {
			this.resolver = resolver;
		}

		@Override
		public void handle(String path, Request request,
						   HttpServletRequest httpServletRequest,
						   HttpServletResponse httpServletResponse)
				throws IOException, ServletException {
			LOG.log(Level.FINE, "Request :" + path);
			Map<String, String[]> params = request.getParameterMap();
			int count = params.containsKey(COUNT_PARAM)
					? Integer.parseInt(request.getParameter(COUNT_PARAM))
					: 1;
			if (params.containsKey(Q_PARAM)){
				List<String> locationNames = Arrays.asList(params.get(Q_PARAM));
				HashMap<String, List<String>> result = resolver.searchGeoName(locationNames, count);
				try (PrintStream out = new PrintStream(httpServletResponse.getOutputStream())) {
					writeResult(result, out);
				}
				httpServletResponse.setStatus(HttpStatus.OK_200);
				httpServletResponse.setContentType("application/json");
			} else {
				httpServletResponse.setStatus(HttpStatus.BAD_REQUEST_400);
			}
			request.setHandled(true);
		}
	}

	/**
	 * Launches embedded jetty on the specified port
	 * @param port port number
	 * @param geoNameResolver name resolver
	 * @throws Exception when an error occurs
     */
	public static void launchService(int port, GeoNameResolver geoNameResolver) throws Exception {
		System.out.println("Launching Service on Port : " + port);
		Server server = new Server(port);
		server.setHandler(new SearchHandler(geoNameResolver));
		server.start();
		server.join();
	}

	/**
	 * Writes the result to given PrintStream
	 * @param resolvedEntities map of resolved entities
	 * @param out the print stream for writing output
	 */
	private static void writeResult(Map<String, List<String>> resolvedEntities, PrintStream out) {
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

		String indexPath = null;
		String gazetteerPath = null;
		Options options = new Options();
		options.addOption(buildOpt);
		options.addOption(searchOpt);
		options.addOption(indexOpt);
		options.addOption(helpOpt);
		options.addOption(resultCountOpt);
		options.addOption(serverOption);

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

				Map<String, List<String>> resolved = resolver
						.searchGeoName(indexPath, geoTerms, count);
				writeResult(resolved, System.out);
			} else if (line.hasOption("server")){
				if (indexPath == null) {
					System.err.println("Index path is required");
					System.exit(-2);
				}
				int port = 8765;
				launchService(port, new GeoNameResolver(indexPath));
			} else {
				System.err.println("Sub command not recognised");
				System.exit(-1);
			}

		} catch (ParseException exp) {
			// oops, something went wrong
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
		}
	}

}
