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
import static com.basho.riak.client.convert.reflect.ClassUtil.setFieldValue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.basho.riak.client.convert.UsermetaField;

/**
 * Class that contains the Riak annotated fields for an annotated class
 * 
 * @author russell
 * 
 */
public class AnnotationInfo {

    /**
     * 
     */
    private static final String NO_RIAK_KEY_FIELD_PRESENT = "no riak key field present";
    private final Field riakKeyField;
    private final List<UsermetaField> usermetaItemFields;
    private final Field usermetaMapField;

    /**
     * @param riakKeyField
     * @param usermetaItemFields
     * @param usermetaMapField
     */
    public AnnotationInfo(Field riakKeyField, List<UsermetaField> usermetaItemFields, Field usermetaMapField) {
        this.riakKeyField = riakKeyField;
        this.usermetaItemFields = usermetaItemFields;
        this.usermetaMapField = usermetaMapField;
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
            usermetaData.put(key, val);
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
        if(usermetaMapField != null) {
            setFieldValue(usermetaMapField, obj, localMetaCopy);
        }
    }

}
