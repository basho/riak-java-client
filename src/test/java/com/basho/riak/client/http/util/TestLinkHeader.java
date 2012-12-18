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
package com.basho.riak.client.http.util;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

import com.basho.riak.client.http.util.LinkHeader;
import java.util.List;

public class TestLinkHeader {

    @Test public void parses_null_and_empty_headers() {
        String header = null;
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        assertEquals(0, links.size());

        header = "";
        links = LinkHeader.parse(header);
        assertEquals(0, links.size());

        header = " ";
        links = LinkHeader.parse(header);
        assertEquals(0, links.size());
    }

    @Test public void ignores_malformed_header_components() {
        final String header = "</res>, ;>>abc, </res2>";
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        assertEquals(2, links.size());
        assertNotNull(links.get("/res"));
        assertNotNull(links.get("/res2"));
    }

    @Test public void ignores_empty_elements() {
        final String header = ",</res>; param=value, ,, </res2>,";
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        
        assertEquals(2, links.size());
        
        assertNotNull(links.get("/res"));
        assertEquals(1, links.get("/res").get(0).keySet().size());
        assertEquals("value", links.get("/res").get(0).get("param"));

        assertNotNull(links.get("/res2"));
        assertEquals(0, links.get("/res2").get(0).keySet().size());
    }

    @Test public void parses_one_link_with_one_parameter() {
        final String header = "</res>; param=value";
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        assertEquals(1, links.size());
        assertNotNull(links.get("/res"));
        assertEquals("value", links.get("/res").get(0).get("param"));
    }

    @Test public void parses_multiple_links() {
        final String header = "</res>; param=value, </res2>; param2=value2, </another/res>; param3=value3";
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        assertEquals(3, links.size());
        assertNotNull(links.get("/res"));
        assertNotNull(links.get("/res2"));
        assertNotNull(links.get("/another/res"));
        assertEquals("value", links.get("/res").get(0).get("param"));
        assertEquals("value2", links.get("/res2").get(0).get("param2"));
        assertEquals("value3", links.get("/another/res").get(0).get("param3"));
    }

    @Test public void handles_varying_whitespace() {
        final String header = "</res>;param=value;param2=value2,</res2>;   param3=value3;  param4=value4,    </another/res>";
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        assertEquals(3, links.size());
        assertNotNull(links.get("/res"));
        assertNotNull(links.get("/res2"));
        assertNotNull(links.get("/another/res"));
        assertEquals("value", links.get("/res").get(0).get("param"));
        assertEquals("value2", links.get("/res").get(0).get("param2"));
        assertEquals("value3", links.get("/res2").get(0).get("param3"));
        assertEquals("value4", links.get("/res2").get(0).get("param4"));
        assertEquals(0, links.get("/another/res").get(0).keySet().size());
    }
    
    @Test public void handles_malformed_links() {
        String header = "; param=value";
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        assertEquals(0, links.size());
        
        header = "<>/res>; param=value";
        links = LinkHeader.parse(header);
        assertEquals(0, links.size());

        header = "<; param=value";
        links = LinkHeader.parse(header);
        assertEquals(0, links.size());
    }

    @Test public void parses_link_with_no_parameters() {  
        String header = "</res>";
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        assertEquals(1, links.size());
        assertEquals(0, links.get("/res").get(0).keySet().size());
    }

    @Test public void parses_link_with_multiple_parameters() {  
        String header = "</res>; param1=value1; param2=value2; param3=value3";
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        assertEquals(1, links.size());
        assertEquals(3, links.get("/res").get(0).keySet().size());
        assertEquals("value1", links.get("/res").get(0).get("param1"));
        assertEquals("value2", links.get("/res").get(0).get("param2"));
        assertEquals("value3", links.get("/res").get(0).get("param3"));
    }

    @Test public void parses_link_with_quoted_parameter() {
        String header = "</res>; param1=\"\\\"\\value1\\\"\"";   // backslash escaped quote and 'v' in quoted string
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        assertEquals(1, links.size());
        assertEquals("\"value1\"", links.get("/res").get(0).get("param1"));
    }

    @Test public void maps_valueless_parameter_to_null() {
        final String header = "</res>; param";
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        
        assertTrue(links.get("/res").get(0).keySet().contains("param"));
        assertEquals(null, links.get("/res").get(0).get("param"));
    }

    @Test public void ignores_links_with_malformed_parameters() {
        String header = "</res>; param=, </res2>";
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        assertEquals(1, links.size());
        assertNotNull(links.get("/res2"));

        header = "</res>; @, </res2>";
        links = LinkHeader.parse(header);
        assertEquals(1, links.size());
        assertNotNull(links.get("/res2"));

        header = "</res>; \"param\", </res2>";
        links = LinkHeader.parse(header);
        assertEquals(1, links.size());
        assertNotNull(links.get("/res2"));
    }
    
    @Test public void parse_multiple_links_to_same_object() {
        String header = "</res>; param=foo, </res>; param=bar";
        Map<String, List<Map<String, String>>> links = LinkHeader.parse(header);
        assertEquals(1, links.size());
        List<Map<String,String>> paramMaps = links.get("/res");
        assertEquals(2, paramMaps.size());
        Map<String,String> map1 = paramMaps.get(0);
        assertEquals(map1.get("param"), "foo");
        
        
    }
    
}
