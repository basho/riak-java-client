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
        if (method == null || method.getAnnotation(RiakIndex.class) == null
            || "".equals(method.getAnnotation(RiakIndex.class).name())
            || (!method.getReturnType().equals(String.class)
            && !method.getReturnType().equals(Integer.class)
            && !method.getReturnType().equals(int.class))
            && !method.getReturnType().equals(Long.class)
            && !method.getReturnType().equals(long.class)
            && !Set.class.isAssignableFrom(method.getReturnType()))
        {
            throw new IllegalArgumentException(method.getReturnType().toString());
        }

        if (Set.class.isAssignableFrom(method.getReturnType()))
        {
            // Verify it's a Set<String> or Set<Integer>
            final Type t = method.getGenericReturnType();
            if (t instanceof ParameterizedType)
            {
                final Class<?> genericType = (Class<?>) ((ParameterizedType) t).getActualTypeArguments()[0];
                if (!genericType.equals(String.class) && !genericType.equals(Integer.class))
                {
                    throw new IllegalArgumentException(method.getReturnType().toString());
                }
            }
            else
            {
                throw new IllegalArgumentException(method.getReturnType().toString());
            }
        }
        this.method = method;
        this.indexName = method.getAnnotation(RiakIndex.class).name();
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
}
