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
 * Annotation to declare a field as holding a collection of RiakLinks
 * <p>
 * Annotate a single field in your domain class, it must be a
 * Collection<RiakLink> This is so ORM features can still be used and RiakLink
 * data made available. At a later date this will be used to model
 * relationships/graphs of domain objects, for now it sticks a Riak domain thing
 * in your domain.
 * </p>
 * <p>
 * For example: <code><pre>
 * public class MyClass {
 *     \@RiakKey
 *     private String myKeyString;
 *     
 *     \@RiakLinks
 *     private Collection<RiakLinks> links;
 * }
 * </pre></code>
 * </p>
 * 
 * 
 * @author russell
 * @see JSONConverter
 * @see DefaultBucket
 */
@Retention(RetentionPolicy.RUNTIME) @Target(ElementType.FIELD) public @interface RiakLinks {
}
