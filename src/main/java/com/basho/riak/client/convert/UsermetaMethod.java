/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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

package com.basho.riak.client.convert;

import java.lang.reflect.Method;
import java.util.Map;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class UsermetaMethod
{
    private final Method method;
    private final String usermetaDataKey;
    
    public UsermetaMethod(Method method) 
    {
        if (null == method || 
            method.getAnnotation(RiakUsermeta.class) == null ||
            ( 
                (method.getReturnType().equals(String.class) && 
                 "".equals(method.getAnnotation(RiakUsermeta.class).key())) &&
                !method.getReturnType().equals(Void.TYPE) && 
                !Map.class.isAssignableFrom(method.getReturnType())
            )
          )
        {
            throw new IllegalArgumentException();
        }
        else
        {
            this.method = method;
            this.usermetaDataKey = method.getAnnotation(RiakUsermeta.class).key();
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
    
}
