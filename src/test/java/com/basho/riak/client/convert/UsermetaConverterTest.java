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
package com.basho.riak.client.convert;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author russell
 * 
 */
public class UsermetaConverterTest {

    /**
     * 
     */
    private static final String META_KEY_ONE = "metaKeyOne";
    private UsermetaConverter<DomainObject> converter;

    @Before public void setUp() {
        this.converter = new UsermetaConverter<DomainObject>();
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.convert.UsermetaConverter#getUsermetaData(java.lang.Object)}
     * .
     */
    @Test public void getPopulatedUsermeta() {
        final String userMetaItemOne = "userMetaItemOne";
        final Map<String, String> userMetaData = makeMap(vargs("key2", "key3", "key4"), vargs("val2", "val3", "val4"));
        final DomainObject obj = new DomainObject();

        obj.setMetaItemOne(userMetaItemOne);
        obj.setUsermetaData(userMetaData);

        Map<String, String> actual = converter.getUsermetaData(obj);

        for (Map.Entry<String, String> e : userMetaData.entrySet()) {
            assertTrue("Expected key " + e.getKey() + " to be present", actual.containsKey(e.getKey()));
            assertEquals(e.getValue(), actual.get(e.getKey()));
        }

        assertTrue(actual.containsKey(META_KEY_ONE));
        assertEquals(userMetaItemOne, actual.get(META_KEY_ONE));

    }

    /**
     * @param keys
     * @param values
     * @return
     */
    private Map<String, String> makeMap(String[] keys, String[] values) {
        final Map<String, String> map = new LinkedHashMap<String, String>();
        for (int i = 0; i < keys.length; i++) {
            if (values.length >= i) {
                map.put(keys[i], values[i]);
            }
        }

        return map;
    }

    /**
     * @param <T>
     * @param args
     * @return
     */
    private <T> T[] vargs(T... args) {
        return args;
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.convert.UsermetaConverter#populateUsermeta(java.util.Map, java.lang.Object)}
     * .
     */
    @Test public void testPopulateUsermeta() {
        final String userMetaItemOne = "userMetaItemOne";
        final Map<String, String> usermetaData = makeMap(vargs(META_KEY_ONE, "key2", "key3", "key4"),
                                                         vargs(userMetaItemOne, "val2", "val3", "val4"));

        final Map<String, String> expected = new HashMap<String, String>(usermetaData);
        expected.remove(META_KEY_ONE);

        DomainObject obj = new DomainObject();

        obj = converter.populateUsermeta(usermetaData, obj);

        Map<String, String> actual = obj.getUsermetaData();

        assertNotNull("Expected meta data field to be populated", actual);

        for (Map.Entry<String, String> e : expected.entrySet()) {
            assertTrue("Expected key " + e.getKey() + " to be present", actual.containsKey(e.getKey()));
            assertEquals(e.getValue(), actual.get(e.getKey()));
        }

        assertEquals(userMetaItemOne, obj.getMetaItemOne());
    }

    private static final class DomainObject {

        @RiakUsermeta(key = META_KEY_ONE) private String metaItemOne;

        @RiakUsermeta private Map<String, String> usermetaData;

        /**
         * @return the metaItemOne
         */
        public String getMetaItemOne() {
            return metaItemOne;
        }

        /**
         * @param metaItemOne
         *            the metaItemOne to set
         */
        public void setMetaItemOne(String metaItemOne) {
            this.metaItemOne = metaItemOne;
        }

        /**
         * @return the usermetaData
         */
        public Map<String, String> getUsermetaData() {
            return usermetaData;
        }

        /**
         * @param usermetaData
         *            the usermetaData to set
         */
        public void setUsermetaData(Map<String, String> usermetaData) {
            this.usermetaData = usermetaData;
        }
    }

}
