/*
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
package com.basho.riak.client.convert.reflection;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;

import com.basho.riak.client.annotations.RiakIndex;

/**
 *
 * @author gmedina
 */
public class RiakIndexMethod
{
    public enum MethodType { LONG_SETTER, LONG_GETTER, SET_LONG_GETTER, SET_LONG_SETTER, 
                            STRING_GETTER, STRING_SETTER, SET_STRING_GETTER, SET_STRING_SETTER }
    
    private final Method method;
    private final String indexName;
    private final MethodType methodType;

    /**
     * The method that is to be wrapped
     *
     * @param method
     */
    public RiakIndexMethod(final Method method)
    {
        methodType = validateAndGetReturnType(method);
        
        this.method = method;
        this.indexName = method.getAnnotation(RiakIndex.class).name();
        
        if (indexName.isEmpty())
        {
            throw new IllegalArgumentException("@RiakIndex must have 'name' parameter");
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
     * @return the indexName
     */
    public String getIndexName()
    {
        return indexName;
    }

    /**
     * @return the type
     */
    public MethodType getMethodType()
    {
        return methodType;
    }
    
    private MethodType validateAndGetReturnType(Method m)
    {
        if (m != null)
        {
            if (m.getReturnType().equals(Void.TYPE))
            {
                // It's a setter
                Type[] genericParameterTypes = m.getGenericParameterTypes();
                Type t = genericParameterTypes[0];
                if (t instanceof ParameterizedType)
                {
                    ParameterizedType pType = (ParameterizedType)t;
                    if (pType.getRawType().equals(Set.class))
                    {
                        Class<?> genericType = (Class<?>)pType.getActualTypeArguments()[0];
                        if (String.class.equals(genericType))
                        {
                            return MethodType.SET_STRING_SETTER;
                        }
                        else if (Long.class.equals(genericType))
                        {
                            return MethodType.SET_LONG_SETTER;
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
                    else if (t.equals(Long.class) || t.equals(long.class))
                    {
                        return MethodType.LONG_SETTER;
                    }
                }
                throw new IllegalArgumentException("@RiakIndex setter must take a single or Set<> of String or Long: " + m);
            }
            else 
            {
                // It's a getter
                Type t = m.getGenericReturnType();
                if (t instanceof ParameterizedType)
                {
                    ParameterizedType pType = (ParameterizedType)t;
                    if (pType.getRawType().equals(Set.class))
                    {
                        Class<?> genericType = (Class<?>)pType.getActualTypeArguments()[0];
                        if (String.class.equals(genericType))
                        {
                            return MethodType.SET_STRING_GETTER;
                        }
                        else if (Long.class.equals(genericType))
                        {
                            return MethodType.SET_LONG_GETTER;
                        }
                    }
                }
                else
                {
                    t = m.getReturnType();
                    if (t.equals(String.class))
                    {
                        return MethodType.STRING_GETTER;
                    }
                    else if (t.equals(Long.class) || t.equals(long.class))
                    {
                        return MethodType.LONG_GETTER;
                    }
                }
                throw new IllegalArgumentException("@RiakIndex getter must return a single or Set<> of String or Long: " +m );
            }
        }
        throw new IllegalArgumentException("Method can not be null.");
    }
}
