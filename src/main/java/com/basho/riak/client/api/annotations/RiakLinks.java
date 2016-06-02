/*
 * Copyright 2014 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.api.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a field or getter/setter method pair in a class to serve as a set of links.
 * <p>
 * The type must be {@literal Collection<RiakLink>}. 
 * <pre>
 * class MyPojo
 * {
 *     {@literal @}RiakLinks
 *     {@literal Collection<RiakLink>} riakLinks;
 * }
 * 
 * class MyPojo
 * {
 *     private {@literal Collection<RiakLink>} riakLinks;
 * 
 *     {@literal @}RiakLinks
 *     public void setRiakLinks({@literal Collection<RiakLink>} riakLinks)
 *     {
 *         this.riakLinks = riakLinks;
 *     }
 * 
 *     {@literal @}RiakLinks
 *     public {@literal Collection<RiakLink>} getRiakLinks()
 *     {
 *         return riakLinks;
 *     }
 * }
 * 
 * </pre>
 * 
 * 
 * @author Russell Brown <russelldb at basho dot com>
 * @author Brian Roach <roach at basho dot com>
 * @since 1.0
*/
@Retention(RetentionPolicy.RUNTIME) @Target({ElementType.FIELD, ElementType.METHOD}) public @interface RiakLinks
{
}
