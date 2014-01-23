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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.basho.riak.client.bucket.DefaultBucket;

/**
 * Annotation to declare a field or method as the key to a data item in Riak.
 * <p>
 * This annotation can be used either on a String field, or getter/setter pair of methods. 
 * <p>
 * <pre>
 * public class MyPojo {
 *     &#064;RiakKey public String key;
 * }
 * 
 * public class AnotherPojo {
 *     private String key;
 *     
 *     &#064;RiakKey public String getKey() {
 *         return key;
 *     }
 * 
 *     &#064;RiakKey public void setKey(String key) {
 *         this.key = key;
 *     }
 * }
 * </pre>
 * @author russell
 * @author Brian Roach <roach at basho dot com>
 * @see JSONConverter
 * @see DefaultBucket
 */
@Retention(RetentionPolicy.RUNTIME) @Target({ElementType.FIELD, ElementType.METHOD}) public @interface RiakKey {

}
