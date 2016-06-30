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
package com.basho.riak.client.api.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a field or getter/setter method pair in a class to serve as containing user meta data for a Riak
 * object.
 * <p>
 * If you set the key value (to anything other than the empty string) then you
 * can use the annotation to map a single key of user meta data to a field.
 * </p>
 * <p>
 * <pre>
 * public class MyClass 
 * {
 *     
 *     {@literal @}RiakUsermeta
 *     private {@literal Map<String, String>} usermetaData;
 *     
 *     {@literal @}RiakUsermeta("usermeta-data-key1") 
 *     private String usermetaDataItem1;
 * }
 * 
 * public class MyClass
 * {
 *     private {@literal Map<String, String>} usermetaData;
 *     private String usermetaDataItem1;
 * 
 *     {@literal @}RiakUsermeta
 *     public {@literal Map<String, String>} getMeta()
 *     {
 *         return usermetaData;
 *     }
 * 
 *     {@literal @}RiakUsermeta
 *     public void setMeta({@literal Map<String,String>} meta)
 *     {
 *         usermetaData = meta;
 *     }
 * 
 *     {@literal @}RiakUsermeta("usermeta-data-key1") 
 *     public String getSingleMeta()
 *     {
 *          return usermetaDataItem1;
 *     }
 * 
 *     {@literal @}RiakUsermeta("usermeta-data-key1")
 *     public void setSingleMeta(String meta)
 *     {
 *         usermetaDataItem1 = meta;
 *     }
 *  }
 * </pre>
 * </p>
 * 
 * 
 * @author Russel Brown <russelldb at basho dot com>
 * @author Brian Roach <roach at basho dot com>
 * @since 1.0
 */
@Retention(RetentionPolicy.RUNTIME) @Target({ElementType.FIELD, ElementType.METHOD}) public @interface RiakUsermeta
{
    /**
     * Use a lower case key. The riak HTTP API *will* lower case key names.
     * @return the key for the user meta item
     */
    String key() default "";
}
