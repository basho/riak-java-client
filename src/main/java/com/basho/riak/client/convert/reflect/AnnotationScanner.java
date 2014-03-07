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
package com.basho.riak.client.convert.reflect;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import com.basho.riak.client.convert.RiakIndex;
import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.convert.RiakLinks;
import com.basho.riak.client.convert.RiakTombstone;
import com.basho.riak.client.convert.RiakUsermeta;
import com.basho.riak.client.convert.RiakVClock;
import java.util.LinkedList;

/**
 * A {@link Callable} that loops over a classes fields and pulls out the fields
 * for {@link RiakUsermeta} and {@link RiakKey}
 * 
 * @author russell
 * 
 */
public class AnnotationScanner implements Callable<AnnotationInfo> {

    @SuppressWarnings("rawtypes") private final Class classToScan;

    @SuppressWarnings("rawtypes") public AnnotationScanner(Class clazz) {
        this.classToScan = clazz;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public AnnotationInfo call() throws Exception {
        
        AnnotationInfo.Builder builder = new AnnotationInfo.Builder();

        // This allows us to start at the top and walk down so that
        // annotations are overridden in subsclasses
        LinkedList<Class> classList = new LinkedList<Class>();
        Class currentClass = classToScan;
        while (currentClass != Object.class) {
            classList.addFirst(currentClass);
            currentClass = currentClass.getSuperclass();
        }
        
        for (Class c : classList) {

            final Field[] fields = c.getDeclaredFields();

            for (Field field : fields) {

                if (field.isAnnotationPresent(RiakKey.class)) {
                    builder.withRiakKeyField(field);
                } else if (field.isAnnotationPresent(RiakVClock.class)) {
                    builder.withRiakVClockField(field);
                } else if (field.isAnnotationPresent(RiakTombstone.class)) {
                    builder.withRiakTombstoneField(field);
                } else if (field.isAnnotationPresent(RiakUsermeta.class)) {
                    builder.addRiakUsermetaField(field);
                } else if(field.isAnnotationPresent(RiakIndex.class)) {
                    builder.addRiakIndexField(field);
                } else if (field.isAnnotationPresent(RiakLinks.class)) {
                    builder.withRiakLinksField(field);
                }
            }
        
        
            final Method[] methods = c.getDeclaredMethods();
            for (Method method : methods) {

                if (method.isAnnotationPresent(RiakIndex.class)) {
                    builder.addRiakIndexMethod(method);
                } else if (method.isAnnotationPresent(RiakUsermeta.class)) {
                    builder.addRiakUsermetaMethod(method);
                } else if (method.isAnnotationPresent(RiakKey.class)) {
                    if (isSetter(method)) {
                        builder.withRiakKeySetter(method);
                    } else {
                        builder.withRiakKeyGetter(method);
                    }
                } else if (method.isAnnotationPresent(RiakLinks.class)) {
                    if (isSetter(method)) {
                        builder.withRiakLinksSetter(method);
                    } else {
                        builder.withRiakLinksGetter(method);
                    } 
                } else if (method.isAnnotationPresent(RiakVClock.class)) {
                    if (isSetter(method)) {
                        builder.withRiakVClockSetter(method);
                    } else {
                        builder.withRiakVClockGetter(method);
                    }
                } else if (method.isAnnotationPresent(RiakTombstone.class)) {
                    if (isSetter(method)) {
                        builder.withRiakTombstoneSetterMethod(method);
                    } else {
                        builder.withRiakTombstoneGetterMethod(method);
                    }
                }
            }
        }
        
        return builder.build();
    }
    
    private boolean isSetter(Method m) {
        return m.getReturnType().equals(Void.TYPE);
    }
}
