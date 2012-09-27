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
import java.lang.reflect.Member;

/**
 * Reflection/class utilities. Delegates to
 * {@link com.fasterxml.jackson.databind.util.ClassUtil}.
 * 
 * @author russell
 * 
 */
public final class ClassUtil {

    private ClassUtil() {}

    /**
     * Make the {@link Member} accessible if possible, throw
     * {@link IllegalArgumentException} is not.
     * 
     * @param member
     * @throw {@link IllegalArgumentException} if cannot set accessibility
     */
    public static <T extends Member> T checkAndFixAccess(T member) {
        com.fasterxml.jackson.databind.util.ClassUtil.checkAndFixAccess(member);
        return member;
    }

    public static void setFieldValue(Field field, Object obj, Object value) {
        try {
            field.set(obj, value);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Unable to set Riak annotated field value", e);
        }
    }

    public static <T> Object getFieldValue(Field f, T obj) {
        Object value = null;
        try {
            value = f.get(obj);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Unable to get Riak annotated field value", e);
        }

        return value;
    }

}
