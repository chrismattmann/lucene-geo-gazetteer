Lucene Geo-Gazetteer 
==================== 
A command line gazetteer
built around the [Geonames.org](http://geonames.org/) dataset, that
uses the [Apache Lucene](http://lucene.apache.org/) library to
create a searchable gazetter.


Use 
=== 
The [Geonames.org](http://download.geonames.org/export/dump/)
dataset contains over 10,000,000 geographical names corresponding
to over 7,500,000 unique features. Beyond names of places in various
languages, data stored include latitude, longitude, elevation,
population, administrative subdivision and postal codes. All
coordinates use the World Geodetic System 1984 (WGS84).

1. What we need here is to download the latest version of
allCountries.zip file from GeoNames.org: `curl -O
http://download.geonames.org/export/dump/allCountries.zip` 
2. and
unzip the GeoNames file: `unzip allCountries.zip` 
3. Take the
_allCountries.txt_ and use it to create a geoIndex: `java -cp
target/lucene-geo-gazetteer-<version>-jar-with-dependencies.jar
edu.usc.ir.geo.gazetteer.GeoNameResolver -i geoIndex -b allCountries.txt`
4. Then search the index (e.g., for Pasadena and Texas): `  java
-cp target/lucene-geo-gazetteer-<version>-jar-with-dependencies.jar
edu.usc.ir.geo.gazetteer.GeoNameResolver -i geoIndex -s Pasadena
Texas`
5. The service mode:
  ```
  #Launch Server
  $ lucene-geo-gazetteer -server
  # Query
  $ curl "localhost:8765/api/search?s=Pasadena&s=Texas&c=2"
  ```

Questions, comments?  
=================== 
Send them to [Chris A. Mattmann](mailto:chris.a.mattmann@jpl.nasa.gov).

Contributors 
============ 
* Thamme Gowda N., USC
* Madhav Sharan, USC
* Yun Li, USC 
* Chris A. Mattmann, JPL

Credits 
======= 
This project began as the [CSCI 572](http://sunset.usc.edu/classes/cs572_2015/) project
of [Yun  Li](https://github.com/AranyaLi) on the NSF
Polar CyberInfrastructure project at USC under the supervision
of Chris Mattmann. You can find Yun's original code base
[here](https://github.com/AranyaLi/GeoParsingNSF).

This work was sponsored by the [National Science Foundation](http://www.nsf.gov/) under funded projects
[PLR-1348450](http://www.nsf.gov/awardsearch/showAward?AWD_ID=1348450&HistoricalAwards=false)
and
[PLR-144562](http://www.nsf.gov/awardsearch/showAward?AWD_ID=1445624&HistoricalAwards=false).

License 
======= 
[Apache License, version 2](http://www.apache.org/licenses/LICENSE-2.0)
