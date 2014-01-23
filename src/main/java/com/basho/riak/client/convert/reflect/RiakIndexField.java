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

import com.basho.riak.client.convert.RiakIndex;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;

/**
 * @author russell
 * 
 */
public class RiakIndexField {

    private final Field field;
    private final String indexName;
    @SuppressWarnings("rawtypes") private final Class type;

    /**
     * The field that is to be wrapped
     * 
     * @param field
     */
    public RiakIndexField(final Field field) {
        // Supporting int / Integer for legacy. New code should use long / Long
        if (field == null || field.getAnnotation(RiakIndex.class) == null ||
            "".equals(field.getAnnotation(RiakIndex.class).name()) ||
            (!field.getType().equals(String.class) &&
                !field.getType().equals(Integer.class) && 
                !field.getType().equals(int.class) &&
                !field.getType().equals(long.class) &&
                !field.getType().equals(Long.class)) &&
                !Set.class.isAssignableFrom(field.getType())
            ) {
            throw new IllegalArgumentException(field.getType().toString());
        }

        if (Set.class.isAssignableFrom(field.getType())) {
            // Verify it's a Set<String> or Set<Long>. Set<Integer> supported for legacy
            Type t = field.getGenericType();
            if (t instanceof ParameterizedType) {
                Class genericType = (Class)((ParameterizedType)t).getActualTypeArguments()[0];
                if (!genericType.equals(String.class) && 
                    !genericType.equals(Integer.class) && 
                    !genericType.equals(Long.class)) {
                    throw new IllegalArgumentException(field.getType().toString());
                }
            } else {
                throw new IllegalArgumentException(field.getType().toString());
            }
        }
        this.field = field;
        this.indexName = field.getAnnotation(RiakIndex.class).name();
        this.type = field.getType();
    }

    /**
     * @return the field
     */
    public Field getField() {
        return field;
    }

    /**
     * @return the indexName
     */
    public String getIndexName() {
        return indexName;
    }
    
    public Class<?> getType() {
        return type;
    }
}
