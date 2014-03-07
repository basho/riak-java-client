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

import java.lang.reflect.Field;

import com.basho.riak.client.convert.RiakIndex;
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
        validateField(field);
        this.field = field;
        this.indexName = field.getAnnotation(RiakIndex.class).name();
        
        if (indexName.isEmpty())
        {
            throw new IllegalArgumentException("@RiakIndex must have 'name' parameter");
        }
        
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
    
    private void validateField(Field f) {
        
        Type t = f.getGenericType();
        if (t instanceof ParameterizedType)
        {
            ParameterizedType pType = (ParameterizedType)t;
            if (pType.getRawType().equals(Set.class))
            {
                Class<?> genericType = (Class<?>)pType.getActualTypeArguments()[0];
                if (Integer.class.equals(genericType) ||
                    String.class.equals(genericType) ||
                    Long.class.equals(genericType))
                {
                    return;
                }
            }
        }
        else
        {
            t = f.getType();
            
            if (t.equals(Integer.class) ||
                    t.equals(int.class) ||
                    t.equals(String.class) ||
                    t.equals(Long.class) ||
                    t.equals(long.class))
            {
                return;
            }
        }
        throw new IllegalArgumentException("@RiakIndex must be a single or Set<> of int/Integer, long/Long, String: " +
                                            f);

    }
}
