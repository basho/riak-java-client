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
package com.basho.riak.client.api.convert.reflection;

import java.lang.reflect.Field;

import com.basho.riak.client.api.annotations.RiakIndex;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.Set;

/**
 * @author russell
 * 
 */
public class RiakIndexField
{

    public enum FieldType { LONG, SET_LONG, STRING, SET_STRING, RAW, SET_RAW, BIG_INT, SET_BIG_INT }
    
    private final Field field;
    private final String indexName;
    private final FieldType type;

    /**
     * The field that is to be wrapped
     * 
     * @param field
     */
    public RiakIndexField(final Field field)
    {
        type = validateAndGetType(field);
        this.field = field;
        this.indexName = field.getAnnotation(RiakIndex.class).name();
        
        if (indexName.isEmpty())
        {
            throw new IllegalArgumentException("@RiakIndex must have 'name' parameter");
        }
        else if ((type == FieldType.RAW || type == FieldType.SET_RAW) &&
                 (!indexName.endsWith("_int") && !indexName.endsWith("_bin")))
        {
            throw new IllegalArgumentException("@RiakIndex annotated byte[] must declare full indexname");
        }
    }

    /**
     * @return the field
     */
    public Field getField()
    {
        return field;
    }

    /**
     * @return the indexName
     */
    public String getIndexName()
    {
        return indexName;
    }
    
    public FieldType getFieldType()
    {
        return type;
    }
    
    private FieldType validateAndGetType(Field f)
    {
        
        if (f != null)
        {
            Type t = f.getGenericType();
            if (t instanceof ParameterizedType)
            {
                ParameterizedType pType = (ParameterizedType)t;
                if (pType.getRawType().equals(Set.class))
                {
                    Class<?> genericType = (Class<?>)pType.getActualTypeArguments()[0];
                    if (String.class.equals(genericType))
                    {
                        return FieldType.SET_STRING;
                    }
                    else if (Long.class.equals(genericType))
                    {
                        return FieldType.SET_LONG;
                    }
                    else if (BigInteger.class.equals(genericType))
                    {
                        return FieldType.SET_BIG_INT;
                    }
                    else if (genericType.isArray() && genericType.getComponentType().equals(byte.class))
                    {
                        return FieldType.SET_RAW;
                    }
                }
            }
            else
            {
                Class<?> c = f.getType();

                if (c.equals(String.class))
                {
                    return FieldType.STRING;
                }
                else if (c.equals(Long.class) || c.equals(long.class))
                {
                    return FieldType.LONG;
                }
                else if (c.equals(BigInteger.class))
                {
                    return FieldType.BIG_INT;
                }
                else if (c.isArray() && c.getComponentType().equals(byte.class))
                {
                    return FieldType.RAW;
                }
            }
            throw new IllegalArgumentException("@RiakIndex must be a single or Set<> of Long, BigInteger, or String: " +
                                               f);
        }
        throw new IllegalArgumentException("Field can not be null.");
    }
}
