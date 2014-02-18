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
package com.basho.riak.client.operations.mapreduce;

import java.util.Collection;

import com.basho.riak.client.convert.ConversionException;

/**
 * Defines a way to access the results of a Map/reduce query.
 * 
 * @author russell
 * @see MapReduce#execute()
 */
public interface MapReduceResult {

    /**
     * Mapped results to a simple java type, can be a Collection type (Map, List
     * etc) or a Java Bean style class.
     * 
     * NOTE: You can annotate a class with Jackson annotations, too, but that is
     * an implementation detail that may change. Expect a set of annotations
     * that are API specific soon.
     * 
     * @param <T>
     * @param resultType
     *            A Java type to map the result too.
     * @return a Collection of T.
     */
    <T> Collection<T> getResult(Class<T> resultType) throws ConversionException;

    /**
     * The raw JSON string of the result
     * 
     * @return a String of JSON, useful if you need to do your own de-serialization.
     */
    String getResultRaw();
}
