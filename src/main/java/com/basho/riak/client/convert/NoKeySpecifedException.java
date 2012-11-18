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

import javax.annotation.concurrent.Immutable;

import com.basho.riak.client.bucket.DefaultBucket;

/**
 * Thrown by an operation that requires a key but doesn't have one.
 * 
 * @author russell
 * @see JSONConverter
 * @see DefaultBucket
 */
@Immutable
public class NoKeySpecifedException extends RuntimeException {

    /**
     * eclipse generated id
     */
    private static final long serialVersionUID = 8973356637885359438L;
    private final Object domainObject;

    /**
     * Construct an exception, pass the domainObject for which the key cannot be
     * found.
     * 
     * @param domainObject
     *            some object that was attempted to be stored/fetched/deleted
     *            from Riak but has no {@link RiakKey} field.
     */
    public NoKeySpecifedException(final Object domainObject) {
        this.domainObject = domainObject;
    }

    /**
     * Get the object that triggered the exception
     * @return the offending instance.
     */
    public Object getDomainObject() {
        return domainObject;
    }
}
