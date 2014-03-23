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
 * Annotation to declare a field or a getter/setter pair as holding a collection of RiakLinks
 * <p>
 * Annotate a single field in your domain class. It must be a
 * Collection<RiakLink>. This is so ORM features can still be used and RiakLink
 * data made available. 
 * </p>
 * <p>
 * For example: <code><pre>
 * public class MyClass {
 *     \@RiakKey
 *     private String myKeyString;
 *     
 *     \@RiakLinks
 *     private Collection<RiakLink> links;
 * }
 * </pre></code>
 * </p>
 * 
 * 
 * @author russell
 * @see JSONConverter
 */
@Retention(RetentionPolicy.RUNTIME) @Target({ElementType.FIELD, ElementType.METHOD}) public @interface RiakLinks {
}
