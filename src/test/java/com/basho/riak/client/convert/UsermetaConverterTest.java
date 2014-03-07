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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/**
 * @author russell
 * 
 */
public class UsermetaConverterTest extends ConversionUtilTest {

    /**
     * Test method for
     * {@link com.basho.riak.client.convert.UsermetaConverter#getUsermetaData(java.lang.Object)}
     * .
     */
    @Test public void getPopulatedUsermetaField() {
        final String userMetaItemOne = "userMetaItemOne";
        final Map<String, String> userMetaData = makeMap(vargs("key2", "key3", "key4"), vargs("val2", "val3", "val4"));
        final UsermetaConverter<PojoWithAnnotatedFields<Void>> converter =
            new UsermetaConverter<PojoWithAnnotatedFields<Void>>();
        
        PojoWithAnnotatedFields<Void> obj = new PojoWithAnnotatedFields<Void>();
        obj.usermeta = userMetaData;
        obj.metaItemOne = userMetaItemOne;
        
        Map<String, String> actual = converter.getUsermetaData(obj);
        
        assertTrue(actual.entrySet().containsAll(userMetaData.entrySet()));
        assertTrue(actual.containsKey(META_KEY_ONE));
        assertEquals(userMetaItemOne, actual.get(META_KEY_ONE));
        
    }
    
    @Test
    public void getPopulatedUsermetaMethod() {
        final String userMetaItemOne = "userMetaItemOne";
        final Map<String, String> userMetaData = makeMap(vargs("key2", "key3", "key4"), vargs("val2", "val3", "val4"));
        final UsermetaConverter<PojoWithAnnotatedMethods<Void>> converter =
            new UsermetaConverter<PojoWithAnnotatedMethods<Void>>();
        
        PojoWithAnnotatedMethods<Void> obj = new PojoWithAnnotatedMethods<Void>();
        obj.setUsermeta(userMetaData);
        obj.setMetaItemOne(userMetaItemOne);
        
        Map<String, String> actual = converter.getUsermetaData(obj);
        
        assertTrue(actual.entrySet().containsAll(userMetaData.entrySet()));
        assertTrue(actual.containsKey(META_KEY_ONE));
        assertEquals(userMetaItemOne, actual.get(META_KEY_ONE));
    }

    /**
     * @param vargs
     * @param vargs2
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
     * @param string
     * @param string2
     * @param string3
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
    @Test public void testPopulateUsermetaField() {
        final String userMetaItemOne = "userMetaItemOne";
        final Map<String, String> usermetaData = makeMap(vargs(META_KEY_ONE, "key2", "key3", "key4"),
                                                         vargs(userMetaItemOne, "val2", "val3", "val4"));
        final UsermetaConverter<PojoWithAnnotatedFields<Void>> converter =
            new UsermetaConverter<PojoWithAnnotatedFields<Void>>();
        
        final Map<String, String> expected = new HashMap<String, String>(usermetaData);
        expected.remove(META_KEY_ONE);

        PojoWithAnnotatedFields<Void> pojo = new PojoWithAnnotatedFields<Void>();

        pojo = converter.populateUsermeta(usermetaData, pojo);

        Map<String, String> actual = pojo.usermeta;

        assertNotNull("Expected meta data field to be populated", actual);

        assertTrue(actual.entrySet().containsAll(expected.entrySet()));

        assertEquals(userMetaItemOne, pojo.metaItemOne);
    }

    @Test
    public void testPopulateUsermetaMethod() {
        final String userMetaItemOne = "userMetaItemOne";
        final Map<String, String> usermetaData = makeMap(vargs(META_KEY_ONE, "key2", "key3", "key4"),
                                                         vargs(userMetaItemOne, "val2", "val3", "val4"));
        final UsermetaConverter<PojoWithAnnotatedMethods<Void>> converter =
            new UsermetaConverter<PojoWithAnnotatedMethods<Void>>();
        
        final Map<String, String> expected = new HashMap<String, String>(usermetaData);
        expected.remove(META_KEY_ONE);
        
        PojoWithAnnotatedMethods<Void> pojo = new PojoWithAnnotatedMethods<Void>();
        
        pojo = converter.populateUsermeta(usermetaData, pojo);

        Map<String, String> actual = pojo.getUsermeta();

        assertNotNull("Expected meta data field to be populated", actual);

        assertTrue(actual.entrySet().containsAll(expected.entrySet()));

        assertEquals(userMetaItemOne, pojo.getMetaItemOne());
        
    }
    
    

}
