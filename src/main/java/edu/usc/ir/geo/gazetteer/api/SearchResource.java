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

package edu.usc.ir.geo.gazetteer.api;

import edu.usc.ir.geo.gazetteer.GeoNameResolver;
import edu.usc.ir.geo.gazetteer.domain.Location;
import edu.usc.ir.geo.gazetteer.service.Launcher;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

/**
 * SearchResource is a Rest Resource which offers search on geo location name.
 *
 */
@Path("/search")
public class SearchResource {

    public static final String SEARCH = "s";
    public static final String COUNT = "c";
    private static final Logger LOG = Logger.getLogger(SearchResource.class.getName());

    private final GeoNameResolver resolver;

    public SearchResource(){

        String indexPath = System.getProperty(Launcher.INDEX_PATH_PROP);
        if (indexPath == null || indexPath.isEmpty()) {
            throw new IllegalStateException("Set Index Path with system property "
                    + Launcher.INDEX_PATH_PROP);
        }
        try {
            LOG.info("Initialising searcher from index " + indexPath);
            this.resolver = new GeoNameResolver(indexPath);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    public Response getSearchResults(@QueryParam(SEARCH)List<String> search,
                                     @DefaultValue("1") @QueryParam(COUNT) int count)
            throws IOException {

        if (search == null || search.isEmpty()|| count < 1){
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        //TODO: configure JSON mapping
        HashMap<String, List<Location>> result = resolver.searchGeoName(search, count);
        
        try(ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream()) {
            try (PrintStream stream = new PrintStream(arrayOutputStream)) {
                GeoNameResolver.writeResultJson(result, stream);
            }
            return Response
                    .status(Response.Status.OK)
                    .entity(arrayOutputStream.toString(StandardCharsets.UTF_8.name()))
                    .build();
        }

    }

}
