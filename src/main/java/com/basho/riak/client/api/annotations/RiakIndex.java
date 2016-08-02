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
 * Annotates a field or getter/setter method pair in a class to serve as a RiakIndex.
 * <p>
 * You do not need to specify the index type suffix ({@code _bin}/{@code _int}). It will be
 * inferred from the type of the annotated field <b>except when using {@code byte[]}</b>;
 * in this case the full index name including the suffix must be supplied.
 * </p>
 * <p>
 * For {@code _bin} indexes, {@code String} and {@code byte[]} types are supported. 
 * For {@code _int} indexes, {@code Long}, {@code BigInteger} and {@code byte[]} 
 * are supported. This can either be a single instance or a {@code Set}. 
 * </p>
 * <p>
 * <b>Important Note:</b> if there are multiple index keys for the object and the field
 * is a single value rather than a {@code Set}, only a single index key will be 
 * set (the first returned). 
 * </p>
 * <pre>
 * public class MyClass 
 * {
 *     {@literal @}RiakKey
 *     private String myKeyString;
 *     
 *     {@literal @}RiakIndex(name="email")
 *     private {@literal Set<String>} emailAddress; // will be indexed in the email_bin index
 *     
 *     {@literal @}RiakIndex(name="age")
 *     private long age; // will be indexed in the age_int index
 * }
 * 
 * public class MyClass
 * {
 *     private {@literal Set<Long>} categoryIds;
 * 
 *     {@literal @}RiakIndex(name="category_ids")
 *     public {@literal Set<String>} getCategoryIds()
 *     {
 *         return categoryIds;
 *     }
 * 
 *     {@literal @}RiakIndex(name="category_ids")
 *     public void setCategoryIds({@literal Set<String>} ids)
 *     {
 *         categoryIds = ids;
 *     }
 * }
 * 
 * </pre>
 * 
 * @author Russell Brown
 * @author Brian Roach <roach at basho dot com>
 * @since 1.0
*/
@Retention(RetentionPolicy.RUNTIME) @Target({ElementType.FIELD, ElementType.METHOD}) public @interface RiakIndex
{
    /**
     * @return the index name
     */
    String name();
}
