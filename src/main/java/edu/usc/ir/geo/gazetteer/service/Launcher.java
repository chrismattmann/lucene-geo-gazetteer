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

package edu.usc.ir.geo.gazetteer.service;

import edu.usc.ir.geo.gazetteer.GeoNameResolver;
import edu.usc.ir.geo.gazetteer.api.SearchResource;
import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Wrapper;
import org.apache.catalina.startup.Tomcat;
import org.apache.cxf.jaxrs.servlet.CXFNonSpringJaxrsServlet;

import java.io.File;
import java.io.IOException;
/**
 * This is a launcher for starting Embedded tomcat.
 */
public class Launcher {

    public static final String INDEX_PATH_PROP = "index.path";

    public static void launchService(int port, String indexPath)
            throws IOException, LifecycleException {

        Tomcat server = new Tomcat();
        Context context = server.addContext("/", new File(".").getAbsolutePath());
        System.setProperty(INDEX_PATH_PROP, indexPath);

        Wrapper servlet = context.createWrapper();
        servlet.setName("CXFNonSpringJaxrs");
        servlet.setServletClass(CXFNonSpringJaxrsServlet.class.getName());
        servlet.addInitParameter("jaxrs.serviceClasses", SearchResource.class.getName());

        servlet.setLoadOnStartup(1);
        context.addChild(servlet);
        context.addServletMapping("/api/*", "CXFNonSpringJaxrs");

        System.out.println("Starting Embedded Tomcat on port : " + port );
        server.setPort(port);
        server.start();
        server.getServer().await();
    }

    public static void main(String[] args) throws IOException, LifecycleException {
        if (args.length != 2){
            System.err.println("Usage:\n<port> <geo/index/path>");
            return;
        }
        System.out.println("WARN: This is experimental. User  '" + GeoNameResolver.class.getName() + " -server'");
        launchService(Integer.parseInt(args[0]), args[1]);
    }

}
