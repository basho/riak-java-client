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
import java.lang.reflect.Member;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
/**
 * Reflection/class utilities.
 * 
 * @author russell
 * @author Brian Roach <roach at basho dot com>
 * @since 1.0
 */
public final class ClassUtil
{

    private ClassUtil() {}

    /**
     * Make the {@link Member} accessible if possible, throw
     * {@link IllegalArgumentException} is not.
     * 
     * @param member the member to check
     * @return the member
     * @throws IllegalArgumentException if cannot set accessibility
     */
    public static <T extends Member> T checkAndFixAccess(T member)
    {
         com.fasterxml.jackson.databind.util.ClassUtil.checkAndFixAccess(member, false);
        return member;
    }

    public static void setFieldValue(Field field, Object obj, Object value)
    {
        try
        {
            field.set(obj, value);
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalStateException("Unable to set Riak annotated field value", e);
        }
    }

    public static <T> Object getFieldValue(Field f, T obj)
    {
        Object value = null;
        try
        {
            value = f.get(obj);
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalStateException("Unable to get Riak annotated field value", e);
        }

        return value;
    }

    public static <T> Object getMethodValue(Method m, T obj)
    {
        Object value = null;
        try
        {
            value = m.invoke(obj);
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalStateException("Unable to get Riak annotated method value", e);
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalStateException("Unable to get Riak annotated method value", e);
        }
        catch (InvocationTargetException e)
        {
            throw new IllegalStateException("Unable to get Riak annotated method value", e);
        }

        return value;
    }
    
    public static <T> void setMethodValue(Method m, T obj, Object value)
    {
        try
        {
            m.invoke(obj, value);
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalStateException("Unable to set Riak annotated method value", e);
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalStateException("Unable to set Riak annotated method value", e);
        }
        catch (InvocationTargetException e)
        {
            throw new IllegalStateException("Unable to set Riak annotated method value", e);
        }
    }
    
}
