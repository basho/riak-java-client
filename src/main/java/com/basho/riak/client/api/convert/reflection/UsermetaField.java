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

import com.basho.riak.client.api.annotations.RiakUsermeta;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Convenience wrapper for a String field that is annotated with
 * {@link RiakUsermeta}
 * 
 * @author Russell Brown <russelldb at basho dot com>
 * @author Brian Roach <roach at basho dot com>
 */
public class UsermetaField
{
    
    public enum FieldType
    {
        STRING,
        MAP
    }
    
    private final Field field;
    private final String usermetaDataKey;
    private final FieldType fieldType;

    /**
     * The field that is to be wrapped
     * 
     * @param field
     */
    public UsermetaField(final Field field)
    {
        this.fieldType = validateAndGetType(field);
        this.field = field;
        this.usermetaDataKey = field.getAnnotation(RiakUsermeta.class).key();
        
        if (fieldType == FieldType.STRING && "".equals(usermetaDataKey))
        {
            throw new IllegalArgumentException("@RiakUsermeta annotated String must include key: " + field);
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
     *
     * @return the field type
     */
    public FieldType getFieldType()
    {
        return fieldType;
    }
        
    /**
     * @return the usermetaDataKey
     */
    public String getUsermetaDataKey()
    {
        return usermetaDataKey;
    }
    
    private FieldType validateAndGetType(Field f)
    {
        if (f != null)
        {
            Type t = f.getGenericType();
            if (t instanceof ParameterizedType)
            {
                ParameterizedType pType = (ParameterizedType)t;
                if (pType.getRawType().equals(Map.class))
                {
                    Type genericTypes[] = pType.getActualTypeArguments();
                    if (String.class.equals(genericTypes[0]) && String.class.equals(genericTypes[1]))
                    {
                        return FieldType.MAP;
                    }
                }
            
            }
            else
            {
                if (f.getType().equals(String.class)) 
                {
                    return FieldType.STRING;
                }
            }
            
            throw new IllegalArgumentException("@RiakUsermeta must be a Map<String,String> or single String: " +
                                            f);
        }
        throw new IllegalArgumentException("Field can not be null.");
        
    }

}
