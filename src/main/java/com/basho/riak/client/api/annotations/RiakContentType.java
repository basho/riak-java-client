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
 * WITHOUT WARANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.basho.riak.client.api.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;


/**
 * Annotates a field or getter/setter method pair in a class to serve as the content-type.
 * <p>
 * The type must be a {@code String}. 
 * </p>
 * <p>
 * In most cases this is not needed as the {@link com.basho.riak.client.api.convert.Converter}
 * will supply the appropriate content-type during serialization.
 * 
 * <pre>
 * class MyPojo
 * {
 *     {@literal @}RiakContentType
 *     String contentType;
 * }
 * 
 * class MyPojo
 * {
 *     private String contentType;
 * 
 *     {@literal @}RiakContentType
 *     public void setContentType(String contentType)
 *     {
 *         this.contentType = contentType;
 *     }
 * 
 *     {@literal @}RiakContentType
 *     public String getContentType()
 *     {
 *         return contentType;
 *     }
 * }
 * </pre>
 * <p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
@Retention(RetentionPolicy.RUNTIME) @Target({ElementType.FIELD, ElementType.METHOD}) public @interface RiakContentType
{
    
}
