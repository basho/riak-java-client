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

import java.lang.reflect.Field;

/**
 * Static method to get the annotated key from a domain object.
 * @author russell
 * 
 */
public class KeyUtil {

    public static <T> String getKey(T domainObject, String defaultKey) {
        String key = getKey(domainObject);
        if (key == null) {
            key = defaultKey;
        }
        return key;
    }

    public static <T> String getKey(T domainObject) {
        final Field[] fields = domainObject.getClass().getDeclaredFields();

        Object key = null;

        for (Field field : fields) {

            if (field.isAnnotationPresent(RiakKey.class)) {
                boolean oldAccessible = field.isAccessible();
                field.setAccessible(true);
                try {
                    key = field.get(domainObject);
                } catch (IllegalAccessException e) {
                    // NO-OP since we can't get the key
                } finally {
                    field.setAccessible(oldAccessible);
                }

            }
        }

        return key == null ? null : key.toString();
    }

}
