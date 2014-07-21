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
package com.basho.riak.client.api.convert.reflection;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;

import com.basho.riak.client.api.annotations.RiakIndex;
import java.math.BigInteger;

/**
 *
 * @author gmedina
 */
public class RiakIndexMethod
{
    public enum MethodType { LONG_SETTER, LONG_GETTER, SET_LONG_GETTER, SET_LONG_SETTER, 
                            STRING_GETTER, STRING_SETTER, SET_STRING_GETTER, SET_STRING_SETTER,
                            RAW_SETTER, RAW_GETTER, SET_RAW_SETTER, SET_RAW_GETTER,
                            BIG_INT_SETTER, BIG_INT_GETTER, SET_BIG_INT_GETTER, SET_BIG_INT_SETTER
    }
    
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
        else if ( (methodType == MethodType.RAW_GETTER || methodType == MethodType.RAW_SETTER ||
                   methodType == MethodType.SET_RAW_GETTER || methodType == MethodType.SET_RAW_SETTER)
                 && (!indexName.endsWith("_bin") && !indexName.endsWith("_int")) )
        {
            throw new IllegalArgumentException("@RiakIndex annotated byte[] setter/getter must declare full indexname");
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
                        else if (BigInteger.class.equals(genericType))
                        {
                            return MethodType.SET_BIG_INT_SETTER;
                        }
                        else if (genericType.isArray() && genericType.getComponentType().equals(byte.class))
                        {
                            return MethodType.SET_RAW_SETTER;
                        }
                    }
                }
                else 
                {
                    Class<?> c = m.getParameterTypes()[0];
                    if (c.equals(String.class))
                    {
                        return MethodType.STRING_SETTER;
                    }
                    else if (c.equals(Long.class) || c.equals(long.class))
                    {
                        return MethodType.LONG_SETTER;
                    }
                    else if (c.equals(BigInteger.class))
                    {
                        return MethodType.BIG_INT_SETTER;
                    }
                    else if (c.isArray() && c.getComponentType().equals(byte.class))
                    {
                        return MethodType.RAW_SETTER;
                    }
                }
                throw new IllegalArgumentException("@RiakIndex setter must take a single or Set<> of String, Long, or byte[]: " + m);
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
                        else if (BigInteger.class.equals(genericType))
                        {
                            return MethodType.SET_BIG_INT_GETTER;
                        }
                        else if (genericType.isArray() && genericType.getComponentType().equals(byte.class))
                        {
                            return MethodType.SET_RAW_GETTER;
                        }
                    }
                }
                else
                {
                    Class<?> c = m.getReturnType();
                    if (c.equals(String.class))
                    {
                        return MethodType.STRING_GETTER;
                    }
                    else if (c.equals(Long.class) || c.equals(long.class))
                    {
                        return MethodType.LONG_GETTER;
                    }
                    else if (c.equals(BigInteger.class))
                    {
                        return MethodType.BIG_INT_SETTER;
                    }
                    else if (c.isArray() && c.getComponentType().equals(byte.class))
                    {
                        return MethodType.RAW_GETTER;
                    }
                }
                throw new IllegalArgumentException("@RiakIndex getter must return a single or Set<> of String, Long, or byte[]: " +m );
            }
        }
        throw new IllegalArgumentException("Method can not be null.");
    }
}
