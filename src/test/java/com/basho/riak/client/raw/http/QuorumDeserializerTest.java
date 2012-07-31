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
import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;

/**
 * @author russell
 *
 */
public class QuorumDeserializerTest {

    private ObjectMapper objectMapper;
    
    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new SimpleModule("test_quorm_deserialzer", new Version(1, 0, 0, "SNAPSHOT"))
            .addDeserializer(Quorum.class, new QuorumDeserializer()));
    }

    /**
     * Test method for {@link com.basho.riak.client.raw.http.QuorumDeserializer#deserialize(com.fasterxml.jackson.core.JsonParser, com.fasterxml.jackson.databind.DeserializationContext)}.
     */
    @Test public void deserialize() throws Exception {
        final String json = "{\"w\":\"quorum\", \"r\": \"all\", \"rw\": \"default\", \"dw\": \"one\", \"pr\": 3}";
        
        TestQuora tq = objectMapper.readValue(json, TestQuora.class);
        
        assertNotNull(tq);
        assertEquals(new Quorum(Quora.QUORUM), tq.getW());
        assertEquals(new Quorum(Quora.ALL), tq.getR());
        assertEquals(new Quorum(Quora.DEFAULT), tq.getRw());
        assertEquals(new Quorum(Quora.ONE), tq.getDw());
        assertEquals(new Quorum(3), tq.getPr());
    }
    
    private static final class TestQuora {
        private Quorum w;
        private Quorum r;
        private Quorum rw;
        private Quorum dw;
        private Quorum pr;
        /**
         * @return the w
         */
        public Quorum getW() {
            return w;
        }
        /**
         * @param w the w to set
         */
        @SuppressWarnings("unused") public void setW(Quorum w) {
            this.w = w;
        }
        /**
         * @return the r
         */
        public Quorum getR() {
            return r;
        }
        /**
         * @param r the r to set
         */
        @SuppressWarnings("unused") public void setR(Quorum r) {
            this.r = r;
        }
        /**
         * @return the rw
         */
        public Quorum getRw() {
            return rw;
        }
        /**
         * @param rw the rw to set
         */
        @SuppressWarnings("unused") public void setRw(Quorum rw) {
            this.rw = rw;
        }
        /**
         * @return the dw
         */
        public Quorum getDw() {
            return dw;
        }
        /**
         * @param dw the dw to set
         */
        @SuppressWarnings("unused") public void setDw(Quorum dw) {
            this.dw = dw;
        }
        /**
         * @return the pr
         */
        public Quorum getPr() {
            return pr;
        }
        /**
         * @param pr the pr to set
         */
        @SuppressWarnings("unused") public void setPr(Quorum pr) {
            this.pr = pr;
        }
    }
}
