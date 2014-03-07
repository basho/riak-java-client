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
package com.basho.riak.client.convert.reflect;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;

import com.basho.riak.client.convert.RiakIndex;

/**
 *
 * @author gmedina
 */
public class RiakIndexMethod
{

    private final Method method;
    private final String indexName;
    private final Class<?> type;

    /**
     * The method that is to be wrapped
     *
     * @param method
     */
    public RiakIndexMethod(final Method method)
    {
        validateMethod(method);
        
        this.method = method;
        this.indexName = method.getAnnotation(RiakIndex.class).name();
        
        if (indexName.isEmpty())
        {
            throw new IllegalArgumentException("@RiakIndex must have 'name' parameter");
        }
        
        this.type = method.getReturnType();
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
    public Class<?> getType()
    {
        return type;
    }
    
    private void validateMethod(Method m)
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
                    if (String.class.equals(genericType) ||
                        Integer.class.equals(genericType) ||
                        Long.class.equals(genericType))
                    {
                        return;
                    }
                }
            }
            else 
            {
                t = m.getParameterTypes()[0];
                if (t.equals(Integer.class) ||
                    t.equals(int.class) ||
                    t.equals(String.class) ||
                    t.equals(Long.class) ||
                    t.equals(long.class)
                    )
                {
                    return;
                }
            }
            throw new IllegalArgumentException("@RiakIndex setter must take a single or Set<> of String, Long, or Integer: " + m);
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
                t = m.getReturnType();
                if (t.equals(Integer.class) ||
                    t.equals(int.class) ||
                    t.equals(String.class) ||
                    t.equals(Long.class) ||
                    t.equals(long.class)
                    )
                {
                    return;
                }
            }
            throw new IllegalArgumentException("@RiakIndex getter must return a single or Set<> of String, Long, or Integer: " +m );
        }
    }
}
