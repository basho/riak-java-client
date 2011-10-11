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
package com.basho.riak.client.convert;

import java.lang.reflect.Field;

/**
 * Convenience wrapper for a String field that is annotated with
 * {@link RiakUsermeta}
 * 
 * @author russell
 * 
 */
public class UsermetaField {
    private final Field field;
    private final String usermetaDataKey;

    /**
     * The field that is to be wrapped
     * 
     * @param field
     */
    public UsermetaField(final Field field) {
        if (field == null || field.getAnnotation(RiakUsermeta.class) == null ||
            "".equals(field.getAnnotation(RiakUsermeta.class).key()) || !field.getType().equals(String.class)) {
            throw new IllegalArgumentException();
        }
        this.field = field;
        this.usermetaDataKey = field.getAnnotation(RiakUsermeta.class).key();
    }

    /**
     * @return the field
     */
    public Field getField() {
        return field;
    }

    /**
     * @return the usermetaDataKey
     */
    public String getUsermetaDataKey() {
        return usermetaDataKey;
    }

}
