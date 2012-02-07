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
package com.basho.riak.client.cap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * @author russell
 * 
 */
public class QuoraTest {

    /**
     * Test method for {@link com.basho.riak.client.cap.Quora#getValue()}.
     */
    @Test public void testGetValue() {
        assertEquals(-4, Quora.ALL.getValue());
        assertEquals(-2, Quora.ONE.getValue());
        assertEquals(-3, Quora.QUORUM.getValue());
        assertEquals(-5, Quora.DEFAULT.getValue());
        assertEquals(Integer.MIN_VALUE, Quora.INTEGER.getValue());
    }

    /**
     * Test method for {@link com.basho.riak.client.cap.Quora#getName()}.
     */
    @Test public void testGetName() {
        assertEquals("all", Quora.ALL.getName());
        assertEquals("one", Quora.ONE.getName());
        assertEquals("quorum", Quora.QUORUM.getName());
        assertEquals("default", Quora.DEFAULT.getName());
        assertEquals("int", Quora.INTEGER.getName());
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.cap.Quora#fromString(java.lang.String)}.
     */
    @Test public void testFromString() {
        assertEquals(Quora.ALL, Quora.fromString("all"));
        assertEquals(Quora.ONE, Quora.fromString("one"));
        assertEquals(Quora.QUORUM, Quora.fromString("quorum"));
        assertEquals(Quora.DEFAULT, Quora.fromString("default"));
        assertEquals(Quora.INTEGER, Quora.fromString("int"));
        try {
            Quora.fromString("lemon");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // NO-OP
        }
    }
}
