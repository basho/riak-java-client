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
package com.basho.riak.client.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to declare a map field or getter/setter pair as containing user meta data for a Riak
 * object.
 * <p>
 * If you set the key value (to anything other than the empty string) then you
 * can use the annotation to map a single key of user meta data to a field.
 * </p>
 * <p>
 * For example:
 * <code><pre>
 * public class MyClass {
 *     \@RiakKey
 *     private String myKeyString;
 *     
 *     \@RiakUsermeta
 *     private Map<String, String> usermetaData;
 *     // - OR -
 *     \@RiakUsermeta("usermeta-data-key1") 
 *     private String usermetaDataItem1;
 * }
 * </pre></code>
 * </p>
 * 
 * 
 * @author Russel Brown <russelldb at basho dot com>
 * @see JSONConverter
 */
@Retention(RetentionPolicy.RUNTIME) @Target({ElementType.FIELD, ElementType.METHOD}) public @interface RiakUsermeta {
    /**
     * Use a lower case key. The riak HTTP API *will* lower case key names.
     * @return the key for the user meta item
     */
    String key() default "";
}
