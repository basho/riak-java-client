/*
 * Copyright 2014 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.basho.riak.client.api.convert.reflection;

import com.basho.riak.client.api.annotations.RiakUsermeta;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class UsermetaMethod
{
    public enum MethodType { STRING_GETTER, STRING_SETTER, MAP_GETTER, MAP_SETTER }
    
    private final Method method;
    private final String usermetaDataKey;
    private final MethodType methodType;
    
    public UsermetaMethod(Method method) 
    {
        this.methodType = validateAndGetReturnType(method);
        this.method = method;
        this.usermetaDataKey = method.getAnnotation(RiakUsermeta.class).key();
        
        if ((methodType == MethodType.STRING_GETTER || methodType == MethodType.STRING_SETTER) && 
            "".equals(usermetaDataKey))
        {
            throw new IllegalArgumentException("@RiakUsermeta annotated method must include key: " 
                + method);
        }
    }

    /**
     * @return the method
     */
    public Method getMethod()
    {
        return method;
    }

    /**
     * @return the usermetaDataKey
     */
    public String getUsermetaDataKey()
    {
        return usermetaDataKey;
    }
    
    /**
     * Get the return type.
     * @return the return type for this method.
     */
    public MethodType getMethodType()
    {
        return methodType;
    }
    
    private MethodType validateAndGetReturnType(Method m)
    {
        if (m != null)
        {
            if (m.getReturnType().equals(Void.TYPE)) // it's a setter
            {
                Type[] genericParameterTypes = m.getGenericParameterTypes();
                Type t = genericParameterTypes[0];
                if (t instanceof ParameterizedType)
                {
                    ParameterizedType pType = (ParameterizedType)t;
                    if (pType.getRawType().equals(Map.class))
                    {
                        Type genericTypes[] = pType.getActualTypeArguments();
                        if (String.class.equals(genericTypes[0]) && String.class.equals(genericTypes[1]))
                        {
                            return MethodType.MAP_SETTER;
                        }
                    }
                }
                else
                {
                    t = m.getParameterTypes()[0];
                    if (t.equals(String.class))
                    {
                        return MethodType.STRING_SETTER;
                    }
                }
                throw new IllegalArgumentException("@RiakUsermeta setter must take a Map<String,String> or String: " + m);
            }
            else // it's a getter
            {
                Type t = m.getGenericReturnType();
                if (t instanceof ParameterizedType)
                {
                    ParameterizedType pType = (ParameterizedType)t;
                    if (pType.getRawType().equals(Map.class))
                    {
                        Type genericTypes[] = pType.getActualTypeArguments();
                        if (String.class.equals(genericTypes[0]) && String.class.equals(genericTypes[1]))
                        {
                            return MethodType.MAP_GETTER;
                        }
                    }
                
                } 
                else
                {
                    if (m.getReturnType().equals(String.class))
                    {
                        return MethodType.STRING_GETTER;
                    }
                }
                
                throw new IllegalArgumentException("@RiakUsermeta getter must return a Map<String,String> or String: " + m);
            }
        }
        throw new IllegalArgumentException("Method can not be null.");
        
    }
    
}
