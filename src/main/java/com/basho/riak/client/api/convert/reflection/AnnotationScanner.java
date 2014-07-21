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

import com.basho.riak.client.api.annotations.RiakBucketName;
import com.basho.riak.client.api.annotations.RiakBucketType;
import com.basho.riak.client.api.annotations.RiakContentType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import com.basho.riak.client.api.annotations.RiakIndex;
import com.basho.riak.client.api.annotations.RiakKey;
import com.basho.riak.client.api.annotations.RiakLastModified;
import com.basho.riak.client.api.annotations.RiakLinks;
import com.basho.riak.client.api.annotations.RiakTombstone;
import com.basho.riak.client.api.annotations.RiakUsermeta;
import com.basho.riak.client.api.annotations.RiakVClock;
import com.basho.riak.client.api.annotations.RiakVTag;
import java.util.LinkedList;

/**
 * A {@link Callable} that loops over a classes fields and pulls out the fields
 * for {@link RiakUsermeta} and {@link RiakKey}
 *
 * @author russell
 *
 */
public class AnnotationScanner implements Callable<AnnotationInfo>
{

    @SuppressWarnings("rawtypes")
    private final Class classToScan;

    @SuppressWarnings("rawtypes")
    public AnnotationScanner(Class clazz)
    {
        this.classToScan = clazz;
    }

    @Override
    public AnnotationInfo call() throws Exception
    {

        AnnotationInfo.Builder builder = new AnnotationInfo.Builder();

        // This allows us to start at the top and walk down so that
        // annotations are overridden in subsclasses
        LinkedList<Class> classList = new LinkedList<Class>();
        Class currentClass = classToScan;
        while (currentClass != Object.class)
        {
            classList.addFirst(currentClass);
            currentClass = currentClass.getSuperclass();
        }

        for (Class c : classList)
        {

            final Field[] fields = c.getDeclaredFields();

            for (Field field : fields)
            {

                if (field.isAnnotationPresent(RiakKey.class))
                {
                    builder.withRiakKeyField(field);
                }
                else if (field.isAnnotationPresent(RiakVClock.class))
                {
                    builder.withRiakVClockField(field);
                }
                else if (field.isAnnotationPresent(RiakTombstone.class))
                {
                    builder.withRiakTombstoneField(field);
                }
                else if (field.isAnnotationPresent(RiakUsermeta.class))
                {
                    builder.addRiakUsermetaField(field);
                }
                else if (field.isAnnotationPresent(RiakIndex.class))
                {
                    builder.addRiakIndexField(field);
                }
                else if (field.isAnnotationPresent(RiakLinks.class))
                {
                    builder.withRiakLinksField(field);
                }
                else if (field.isAnnotationPresent(RiakContentType.class))
                {
                    builder.withRiakContentTypeField(field);
                }
                else if (field.isAnnotationPresent(RiakLastModified.class))
                {
                    builder.withRiakLastModifiedField(field);
                }
                else if (field.isAnnotationPresent(RiakVTag.class))
                {
                    builder.withRiakVTagField(field);
                }
                else if (field.isAnnotationPresent(RiakBucketName.class))
                {
                    builder.withRiakBucketNameField(field);
                }
                else if (field.isAnnotationPresent(RiakBucketType.class))
                {
                    builder.withRiakBucketTypeField(field);
                }
            }

            final Method[] methods = c.getDeclaredMethods();
            for (Method method : methods)
            {

                if (method.isAnnotationPresent(RiakKey.class))
                {
                    if (isSetter(method))
                    {
                        builder.withRiakKeySetter(method);
                    }
                    else
                    {
                        builder.withRiakKeyGetter(method);
                    }
                }
                else if (method.isAnnotationPresent(RiakBucketName.class))
                {
                    if (isSetter(method))
                    {
                        builder.withRiakBucketNameSetter(method);
                    }
                    else
                    {
                        builder.withRiakBucketNameGetter(method);
                    }
                }
                else if (method.isAnnotationPresent(RiakBucketType.class))
                {
                    if (isSetter(method))
                    {
                        builder.withRiakBucketTypeSetter(method);
                    }
                    else
                    {
                        builder.withRiakBucketTypeGetter(method);
                    }
                }
                else if (method.isAnnotationPresent(RiakLinks.class))
                {
                    if (isSetter(method))
                    {
                        builder.withRiakLinksSetter(method);
                    }
                    else
                    {
                        builder.withRiakLinksGetter(method);
                    }
                }
                else if (method.isAnnotationPresent(RiakVClock.class))
                {
                    if (isSetter(method))
                    {
                        builder.withRiakVClockSetter(method);
                    }
                    else
                    {
                        builder.withRiakVClockGetter(method);
                    }
                }
                else if (method.isAnnotationPresent(RiakTombstone.class))
                {
                    if (isSetter(method))
                    {
                        builder.withRiakTombstoneSetter(method);
                    }
                    else
                    {
                        builder.withRiakTombstoneGetter(method);
                    }
                }
                else if (method.isAnnotationPresent(RiakContentType.class))
                {
                    if (isSetter(method))
                    {
                        builder.withRiakContentTypeSetter(method);
                    }
                    else
                    {
                        builder.withRiakContentTypeGetter(method);
                    }
                }
                else if (method.isAnnotationPresent(RiakVTag.class))
                {
                    if (isSetter(method))
                    {
                        builder.withRiakVTagSetter(method);
                    }
                    else
                    {
                        throw new IllegalArgumentException("@RiakVTag annotated getter not supported");
                    }
                }
                else if (method.isAnnotationPresent(RiakLastModified.class))
                {
                    if (isSetter(method))
                    {
                        builder.withRiakLastModifiedSetter(method);
                    }
                    else
                    {
                        throw new IllegalArgumentException("@RiakLastModified annotated getter not supported");
                    }
                }
                else if (method.isAnnotationPresent(RiakIndex.class))
                {
                    builder.addRiakIndexMethod(method);
                }
                else if (method.isAnnotationPresent(RiakUsermeta.class))
                {
                    builder.addRiakUsermetaMethod(method);
                }
            }
        }

        return builder.build();
    }

    private boolean isSetter(Method m)
    {
        return m.getReturnType().equals(Void.TYPE);
    }
}
