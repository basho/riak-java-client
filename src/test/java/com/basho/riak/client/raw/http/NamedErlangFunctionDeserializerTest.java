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
package com.basho.riak.client.raw.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.query.functions.NamedErlangFunction;

/**
 * @author russell
 * 
 */
public class NamedErlangFunctionDeserializerTest {
    private ObjectMapper objectMapper;

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new SimpleModule("test_quorm_deserialzer", new Version(1, 0, 0, "SNAPSHOT"))
                                    .addDeserializer(NamedErlangFunction.class, new NamedErlangFunctionDeserializer()));
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.http.NamedErlangFunctionDeserializer#deserialize(org.codehaus.jackson.JsonParser, org.codehaus.jackson.map.DeserializationContext)}
     * .
     */
    @Test public void deserialize() throws Exception {
        final String json = "{\"chash_keyfun\":{\"mod\":\"riak_core_util\",\"fun\":\"chash_std_keyfun\"}, \"precommit\": [{\"mod\":\"riak_search_kv_hook\",\"fun\":\"precommit\"}, {\"mod\":\"made_up_mod\",\"fun\":\"made_up_fun\"}]}";

        TestFunctions tf = objectMapper.readValue(json, TestFunctions.class);

        assertEquals(new NamedErlangFunction("riak_core_util", "chash_std_keyfun"), tf.getChash_keyfun());

        assertEquals(2, tf.getPrecommit().size());
        assertTrue(tf.getPrecommit().contains(new NamedErlangFunction("riak_search_kv_hook", "precommit")));
        assertTrue(tf.getPrecommit().contains(new NamedErlangFunction("made_up_mod", "made_up_fun")));
    }

    @Test public void deserialize_empty() throws Exception {
        final String json = "{\"chash_keyfun\":{}, \"precommit\": []}";

        TestFunctions tf = objectMapper.readValue(json, TestFunctions.class);

        assertEquals(null, tf.getChash_keyfun());

        assertEquals(0, tf.getPrecommit().size());
    }

    @Test public void deserialize_absent() throws Exception {
        final String json = "{}";

        TestFunctions tf = objectMapper.readValue(json, TestFunctions.class);

        assertEquals(null, tf.getChash_keyfun());
        assertEquals(null, tf.getPrecommit());
    }

    private static final class TestFunctions {
        private NamedErlangFunction chash_keyfun;
        private List<NamedErlangFunction> precommit;

        /**
         * @return the chash_keyfun
         */
        public NamedErlangFunction getChash_keyfun() {
            return chash_keyfun;
        }

        /**
         * @param chash_keyfun
         *            the chash_keyfun to set
         */
        @SuppressWarnings("unused") public void setChash_keyfun(NamedErlangFunction chash_keyfun) {
            this.chash_keyfun = chash_keyfun;
        }

        /**
         * @return the precommit
         */
        public List<NamedErlangFunction> getPrecommit() {
            return precommit;
        }

        /**
         * @param precommit
         *            the precommit to set
         */
        @SuppressWarnings("unused") public void setPrecommit(List<NamedErlangFunction> precommit) {
            this.precommit = precommit;
        }
    }
}
