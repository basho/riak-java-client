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
 * Annotates a field or getter/setter method pair in a class to serve as the bucket name.
 * <p>
 * The type must be a {@code String}.
 * 
 * <pre>
 * class MyPojo
 * {
 *     {@literal @}RiakBucketName
 *     String bucketName;
 * }
 * 
 * class MyPojo
 * {
 *     private String bucketName;
 * 
 *     {@literal @}RiakBucketName
 *     public void setBucketName(String bucketName)
 *     {
 *         this.bucketName = bucketName;
 *     }
 * 
 *     {@literal @}RiakBucketName
 *     public String getBucketName()
 *     {
 *         return bucketName;
 *     }
 * }
 * </pre>
 * <p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
@Retention(RetentionPolicy.RUNTIME) @Target({ElementType.FIELD, ElementType.METHOD}) public @interface RiakBucketName
{
    
}
