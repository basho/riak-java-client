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
package com.basho.riak.newapi.convert;

import com.basho.riak.newapi.RiakObject;

/**
 * @author russell
 * 
 */
public interface Converter<T> {

    /**
     * Convert from domain specific type to RiakObject 
     * @param domainObject
     * @return a RiakObject populated from domainObject
     */
    RiakObject fromDomain(T domainObject) throws ConversionException;

    /**
     * Convert from a riakObject to a domain specific instance
     * @param riakObject the RiakObject to convert
     * @return an instance of type T
     */
    T toDomain(RiakObject riakObject) throws ConversionException;

}
