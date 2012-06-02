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
package com.basho.riak.client.convert.reflect;

import static com.basho.riak.client.convert.reflect.ClassUtil.getFieldValue;
import static com.basho.riak.client.convert.reflect.ClassUtil.getMethodValue;
import static com.basho.riak.client.convert.reflect.ClassUtil.setFieldValue;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.convert.UsermetaField;
import com.basho.riak.client.query.indexes.RiakIndexes;

/**
 * Class that contains the Riak annotated fields for an annotated class
 * 
 * @author russell
 * 
 */
public class AnnotationInfo {

    private static final String NO_RIAK_KEY_FIELD_PRESENT = "no riak key field present";
    private final Field riakKeyField;
    private final List<UsermetaField> usermetaItemFields;
    private final Field usermetaMapField;
    private final List<RiakIndexField> indexFields;
    private final List<RiakIndexMethod> indexMethods;
    private final Field riakLinksField;

    /**
     * @param riakKeyField
     * @param usermetaItemFields
     * @param usermetaMapField
     */
    public AnnotationInfo(Field riakKeyField, List<UsermetaField> usermetaItemFields, Field usermetaMapField,
            List<RiakIndexField> indexFields, List<RiakIndexMethod> indexMethods, Field riakLinksField) {
        this.riakKeyField = riakKeyField;
        this.usermetaItemFields = usermetaItemFields;
        validateUsermetaMapField(usermetaMapField);
        this.usermetaMapField = usermetaMapField;
        this.indexFields = indexFields;
        this.indexMethods = indexMethods;
        validateRiakLinksField(riakLinksField);
        this.riakLinksField = riakLinksField;
    }

    /**
     * @param riakLinksField
     */
    private void validateRiakLinksField(Field riakLinksField) {
        if (riakLinksField == null) {
            return;
        }

        ParameterizedType type = (ParameterizedType) riakLinksField.getGenericType();
        if (type.getRawType().equals(Collection.class)) {

            Type[] genericParams = type.getActualTypeArguments();
            if (genericParams.length == 1 && genericParams[0].equals(RiakLink.class)) {
                return;
            }
        }
        throw new IllegalArgumentException("riak links field must be Collection<RiakLink>");
    }

    /**
     * @param usermetaMapField
     */
    private void validateUsermetaMapField(Field usermetaMapField) {
        if (usermetaMapField == null) {
            return;
        }

        ParameterizedType type = (ParameterizedType) usermetaMapField.getGenericType();
        if (type.getRawType().equals(Map.class)) {

            Type[] genericParams = type.getActualTypeArguments();
            if (genericParams.length == 2 && genericParams[0].equals(String.class) &&
                genericParams[1].equals(String.class)) {
                return;
            }
        }
        throw new IllegalArgumentException("user meta map field must be Map<String, String>");
    }

    /**
     * @return
     */
    public boolean hasRiakKey() {
        return riakKeyField != null;
    }

    /**
     * @param <T>
     * @param obj
     * @return
     */
    public <T> String getRiakKey(T obj) {
        if (!hasRiakKey()) {
            throw new IllegalStateException(NO_RIAK_KEY_FIELD_PRESENT);
        }

        Object key = getFieldValue(riakKeyField, obj);
        return key == null ? null : key.toString();
    }

    public <T> void setRiakKey(T obj, String key) {
        if (!hasRiakKey()) {
            throw new IllegalStateException(NO_RIAK_KEY_FIELD_PRESENT);
        }

        setFieldValue(riakKeyField, obj, key);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" }) public <T> Map<String, String> getUsermetaData(T obj) {
        final Map<String, String> usermetaData = new LinkedHashMap<String, String>();
        Map<String, String> objectMetaMap = null;

        for (UsermetaField f : usermetaItemFields) {
            Object o = getFieldValue(f.getField(), obj);
            String val = o == null ? null : o.toString();
            String key = f.getUsermetaDataKey();
            // null is not a user meta datum
            if (o != null) {
                usermetaData.put(key, val);
            }
        }

        if (usermetaMapField != null) {
            objectMetaMap = (Map) getFieldValue(usermetaMapField, obj);
        }

        if (objectMetaMap != null) {
            usermetaData.putAll(objectMetaMap);
        }
        return usermetaData;
    }

    public <T> void setUsermetaData(final Map<String, String> usermetaData, T obj) {
        // copy as we will modify
        final Map<String, String> localMetaCopy = new HashMap<String, String>(usermetaData);

        // set any individual annotated fields
        for (UsermetaField f : usermetaItemFields) {
            if (localMetaCopy.containsKey(f.getUsermetaDataKey())) {
                setFieldValue(f.getField(), obj, localMetaCopy.get(f.getUsermetaDataKey()));
                localMetaCopy.remove(f.getUsermetaDataKey());
            }
        }

        // set a catch all map field
        if (usermetaMapField != null) {
            setFieldValue(usermetaMapField, obj, localMetaCopy);
        }
    }

    /**
     * @return a {@link RiakIndexes} made of the values of the RiakIndex
     *         annotated fields and methods. For methods it is expected to be a
     *         Set of String or Integer
     */
    @SuppressWarnings("unchecked") public <T> RiakIndexes getIndexes(T obj) {
        final RiakIndexes riakIndexes = new RiakIndexes();

        for (RiakIndexField f : indexFields) {
            if (Set.class.isAssignableFrom(f.getType())) {
                final Type t = f.getField().getGenericType();
                if (t instanceof ParameterizedType) {
                    final Object val = getFieldValue(f.getField(), obj);
                    if (val != null) {
                        final Class<?> genericType = (Class<?>) ((ParameterizedType) t).getActualTypeArguments()[0];
                        if (String.class.equals(genericType)) {
                            riakIndexes.addBinSet(f.getIndexName(), (Set<String>) val);
                        } else if (Integer.class.equals(genericType)) {
                            riakIndexes.addIntSet(f.getIndexName(), (Set<Integer>) val);
                        }
                    }
                }
            } else {
                final Object val = getFieldValue(f.getField(), obj);
                // null is not an index value
                if (val != null) {
                    if (val instanceof String) {
                        riakIndexes.add(f.getIndexName(), (String) val);
                    } else if (val instanceof Integer) {
                        riakIndexes.add(f.getIndexName(), (Integer) val);
                    }
                }
            }
        }

        for (RiakIndexMethod m : indexMethods) {
            if (Set.class.isAssignableFrom(m.getType())) {
                final Type t = m.getMethod().getGenericReturnType();
                if (t instanceof ParameterizedType) {
                    final Object val = getMethodValue(m.getMethod(), obj);
                    if (val != null) {
                        final Class<?> genericType = (Class<?>) ((ParameterizedType) t).getActualTypeArguments()[0];
                        if (String.class.equals(genericType)) {
                            riakIndexes.addBinSet(m.getIndexName(), (Set<String>) val);
                        } else if (Integer.class.equals(genericType)) {
                            riakIndexes.addIntSet(m.getIndexName(), (Set<Integer>) val);
                        }
                    }
                }
            } else {
                final Object val = getMethodValue(m.getMethod(), obj);
                // null is not an index value
                if (val != null) {
                    if (val instanceof String) {
                        riakIndexes.add(m.getIndexName(), (String) val);
                    } else if (val instanceof Integer) {
                        riakIndexes.add(m.getIndexName(), (Integer) val);
                    }
                }
            }
        }
        return riakIndexes;
    }

    /**
     * TODO Set multi-value indexes for methods when fetched back from Riak
     * 
     * @param <T>
     * @param indexes
     *            the RiakIndexes to copy to the domain object
     * @param obj
     *            the domain object to set indexes on
     */
    public <T> void setIndexes(RiakIndexes indexes, T obj) {
        // copy the index values to the correct fields
        for (RiakIndexField f : indexFields) {
            Set<?> val = null;

            if (Set.class.isAssignableFrom(f.getType())) {
                final Type t = f.getField().getGenericType();
                if (t instanceof ParameterizedType) {
                    final Class<?> genericType = (Class<?>) ((ParameterizedType) t).getActualTypeArguments()[0];
                    if (String.class.equals(genericType)) {
                        val = indexes.getBinIndex(f.getIndexName());
                    } else if (Integer.class.equals(genericType)) {
                        val = indexes.getIntIndex(f.getIndexName());
                    }
                }
                if (val != null && !val.isEmpty()) {
                    setFieldValue(f.getField(), obj, val);
                }
            } else {
                if (Integer.class.equals(f.getType()) || int.class.equals(f.getType())) {
                    val = indexes.getIntIndex(f.getIndexName());
                } else if (String.class.equals(f.getType())) {
                    val = indexes.getBinIndex(f.getIndexName());
                }

                if (val != null && !val.isEmpty()) {
                    setFieldValue(f.getField(), obj, val.iterator().next()); // take
                                                                             // the
                                                                             // first
                                                                             // value
                }
            }
        }
    }

    @SuppressWarnings("unchecked") public <T> Collection<RiakLink> getLinks(T obj) {
        final Collection<RiakLink> links = new ArrayList<RiakLink>();
        if (riakLinksField != null) {
            Object o = getFieldValue(riakLinksField, obj);
            if (o != null && o instanceof Collection) {
                links.addAll((Collection<RiakLink>) o);
            }
        }
        return links;
    }

    public <T> void setLinks(Collection<RiakLink> links, T obj) {
        if (riakLinksField != null) {
            setFieldValue(riakLinksField, obj, links);
        }
    }
}
