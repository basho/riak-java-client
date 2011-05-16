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
 * Static methods to get /set the annotated key from/on a domain object.
 * @author russell
 * @see RiakKey
 * @see JSONConverter
 */
public class KeyUtil {

    /**
     * Attempts to inject <code>key</code> as the value of the {@link RiakKey}
     * annotated field of <code>domainObject</code>
     * 
     * @param <T> the type of <code>domainObject</code>
     * @param domainObject the object to inject the key into
     * @param key the key to inject
     * @return <code>domainObject</code> with {@link RiakKey} annotated field set to <code>key</code>
     * @throws ConversionException if there is a {@link RiakKey} annotated field but it cannot be set to the value of <code>key</code>
     */
    public static <T> T setKey(T domainObject, String key) throws ConversionException {
        final Field[] fields = domainObject.getClass().getDeclaredFields();

        for (Field field : fields) {

            if (field.isAnnotationPresent(RiakKey.class)) {
                boolean oldAccessible = field.isAccessible();
                field.setAccessible(true);
                try {
                    field.set(domainObject, key);
                } catch (IllegalAccessException e) {
                    throw new ConversionException(e);
                } finally {
                    field.setAccessible(oldAccessible);
                }

            }
        }
        return domainObject;
    }

    /**
     * Attempts to get a key from <code>domainObject</code> by looking for a
     * {@link RiakKey} annotated field. If non-present it simply returns
     * <code>defaultKey</code>
     * 
     * @param <T>
     *            the type of <code>domainObject</code>
     * @param domainObject
     *            the object to search for a key
     * @param defaultKey
     *            the pass through value that will get returned if no key found
     *            on <code>domainObject</code>
     * @return either the value found on <code>domainObject</code>;s
     *         {@link RiakKey} field or <code>defaultkey</code>
     */
    public static <T> String getKey(T domainObject, String defaultKey) {
        String key = getKey(domainObject);
        if (key == null) {
            key = defaultKey;
        }
        return key;
    }

    /**
     * Attempts to get a key from <code>domainObject</code> by looking for a
     * {@link RiakKey} annotated field. If non-present it simply returns
     * <code>null</code>
     * 
     * @param <T>
     *            the type of <code>domainObject</code>
     * @param domainObject
     *            the object to search for a key
     * @return either the value found on <code>domainObject</code>;s
     *         {@link RiakKey} field or <code>null</code>
     */
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
