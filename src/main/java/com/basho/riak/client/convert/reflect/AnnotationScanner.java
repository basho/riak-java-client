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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.convert.RiakUsermeta;
import com.basho.riak.client.convert.UsermetaField;

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
        Field riakKeyField = null;
        Field usermetaMapField = null;
        List<UsermetaField> usermetaItemFields = new ArrayList<UsermetaField>();
        final Field[] fields = classToScan.getDeclaredFields();

        for (Field field : fields) {

            if (field.isAnnotationPresent(RiakKey.class)) {
                riakKeyField = ClassUtil.checkAndFixAccess(field);
            }
            if (field.isAnnotationPresent(RiakUsermeta.class)) {
                RiakUsermeta a = field.getAnnotation(RiakUsermeta.class);
                String key = a.key();

                if (!"".equals(key)) {
                    usermetaItemFields.add(new UsermetaField(ClassUtil.checkAndFixAccess(field)));
                } else {
                    usermetaMapField = ClassUtil.checkAndFixAccess(field);
                }

            }
        }
        return new AnnotationInfo(riakKeyField, usermetaItemFields, usermetaMapField);
    }
}
