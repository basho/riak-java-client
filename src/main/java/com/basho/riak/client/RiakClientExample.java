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

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;

import com.basho.riak.client.jiak.JiakClient;
import com.basho.riak.client.jiak.JiakFetchResponse;
import com.basho.riak.client.jiak.JiakObject;
import com.basho.riak.client.jiak.JiakWalkResponse;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RawFetchResponse;
import com.basho.riak.client.raw.RawObject;
import com.basho.riak.client.raw.RawWalkResponse;

public class RiakClientExample {

    public static void main(String[] args) {
        String url = "http://localhost:8098";
        if (args.length > 1) {
            url = args[0];
        }
        System.out.println("Connecting to " + url + "...");

        testJiak(url + "/jiak");
        testRaw(url + "/raw");
        System.out.println("all tests passed");
    }

    public static void testJiak(String url) {
        ArrayList<String> allKeys = new ArrayList<String>();
        allKeys.add("testkey");
        allKeys.add("jroot");
        allKeys.add("jleaf1");
        allKeys.add("jleaf2");
        allKeys.add("jleaf3");

        JiakClient jiak = new JiakClient(url);

        for (String k : allKeys) {
            jiak.delete("jiak_example", k);
        }
        JiakObject jo = new JiakObject("jiak_example", "testkey");
        jo.set("foo", 2);
        jiak.store(jo);
        JiakFetchResponse jfr = jiak.fetch("jiak_example", "testkey");
        assertTrue(jfr.isSuccess() && jfr.hasObject());
        assertTrue(jo.get("foo").equals(2));
        JiakObject jRoot = new JiakObject("jiak_example", "jroot");
        jRoot.set("foo", 0);
        JiakObject jLeaf1 = new JiakObject("jiak_example", "jleaf1");
        jLeaf1.set("foo", "in results");
        JiakObject jLeaf2 = new JiakObject("jiak_example", "jleaf2");
        jLeaf2.set("foo", "in results");
        JiakObject jLeaf3 = new JiakObject("jiak_example", "jleaf3");
        jLeaf3.set("foo", "not in results");
        JSONArray links = new JSONArray();
        links.put(new String[] { "jiak_example", "jleaf1", "tag_one" });
        links.put(new String[] { "jiak_example", "jleaf2", "tag_one" });
        links.put(new String[] { "jiak_example", "jleaf3", "tag_other" });
        jRoot.setLinks(links);
        jiak.store(jRoot);
        jiak.store(jLeaf1);
        jiak.store(jLeaf2);
        jiak.store(jLeaf3);
        JiakWalkResponse jwr = jiak.walk("jiak_example", "jroot", "jiak_example,tag_one,1");
        assertTrue(jwr.isSuccess());
        for (List<JiakObject> i : jwr.getSteps()) {
            for (JiakObject j : i) {
                assertTrue(j.get("foo").equals("in results"));
            }
        }
        for (String k : allKeys) {
            try {
                jiak.delete("jiak_example", k);
            } catch (Exception e) {}
        }
        jiak.store(jLeaf3);
    }

    public static void testRaw(String url) {
        ArrayList<String> allKeys = new ArrayList<String>();
        allKeys.add("testkey");
        allKeys.add("jroot");
        allKeys.add("jleaf1");
        allKeys.add("jleaf2");
        allKeys.add("jleaf3");

        RawClient raw = new RawClient(url);

        for (String k : allKeys) {
            raw.delete("raw_example", k);
        }

        RawObject ro = new RawObject("raw_example", "testkey", "foo");
        raw.store(ro);
        RawFetchResponse jfr = raw.fetch("raw_example", "testkey");
        assertTrue(jfr.isSuccess() && jfr.hasObject());
        assertTrue(ro.getValue().equals("foo"));
        RawObject jRoot = new RawObject("raw_example", "jroot", "foo");
        RawObject jLeaf1 = new RawObject("raw_example", "jleaf1", "in results");
        RawObject jLeaf2 = new RawObject("raw_example", "jleaf2", "in results");
        RawObject jLeaf3 = new RawObject("raw_example", "jleaf3", "no in results");
        List<RiakLink> links = new ArrayList<RiakLink>();
        links.add(new RiakLink("raw_example", "jleaf1", "tag_one"));
        links.add(new RiakLink("raw_example", "jleaf2", "tag_one"));
        links.add(new RiakLink("raw_example", "jleaf3", "tag_other"));
        jRoot.setLinks(links);
        raw.store(jRoot);
        raw.store(jLeaf1);
        raw.store(jLeaf2);
        raw.store(jLeaf3);
        RawWalkResponse jwr = raw.walk("raw_example", "jroot", "raw_example,tag_one,1");
        assertTrue(jwr.isSuccess());
        for (List<RawObject> i : jwr.getSteps()) {
            for (RawObject j : i) {
                assertTrue(j.getValue().equals("in results"));
            }
        }
        for (String k : allKeys) {
            try {
                raw.delete("raw_example", k);
            } catch (Exception e) {}
        }
        raw.store(jLeaf3);
    }

    private static void assertTrue(boolean assertion) {
        if (!assertion)
            throw new AssertionError();
    }
}
