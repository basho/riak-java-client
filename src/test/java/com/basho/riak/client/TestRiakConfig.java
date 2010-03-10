/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestRiakConfig {

    @Test public void chomps_ending_slash() {
        final String URL = "url";
        RiakConfig impl = new RiakConfig(URL + "/");
        assertEquals(URL, impl.getUrl());

        impl.setUrl(URL + "/");
        assertEquals(URL, impl.getUrl());

        impl = new RiakConfig("ip", "port", URL + "/");
        assertFalse(impl.getUrl().endsWith("/"));
    }

    @Test public void builds_correct_url_from_ip_port_and_prefix() {
        RiakConfig impl = new RiakConfig("ip", "port", "/prefix");
        assertEquals("http://ip:port/prefix", impl.getUrl());
    }
    
    @Test public void calculates_correct_base_url() {
        RiakConfig impl = new RiakConfig("http://ip:port/path/to/riak");
        assertEquals("http://ip:port", impl.getBaseUrl());
        
        impl = new RiakConfig("http://ip:port/prefix");
        assertEquals("http://ip:port", impl.getBaseUrl());
        
        impl = new RiakConfig("http://ip:port");
        assertEquals("http://ip:port", impl.getBaseUrl());

        impl = new RiakConfig("http://ip");
        assertEquals("http://ip", impl.getBaseUrl());

        impl = new RiakConfig("ip:port/prefix");
        assertEquals("ip:port", impl.getBaseUrl());

        impl = new RiakConfig("ip/prefix");
        assertEquals("ip", impl.getBaseUrl());

        impl = new RiakConfig("ip");
        assertEquals("ip", impl.getBaseUrl());
    }

    @Test public void calculates_correct_base_mapred_url() {
        RiakConfig impl = new RiakConfig("http://ip:port/path/to/riak");
        assertEquals("http://ip:port/mapred", impl.getMapReduceUrl());

        impl.setMapReducePath("/path/to/mapred/");
        assertEquals("http://ip:port/path/to/mapred", impl.getMapReduceUrl());
    }
}
