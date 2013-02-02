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
package com.basho.riak.client.raw.pbc;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.json.JSONArray;

import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.pbc.MapReduceResponseSource;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Concrete implementation of MapReduceResult that handles PB response stream
 * 
 * @author russell
 * 
 */
public class PBMapReduceResult implements MapReduceResult {

    private final FutureTask<String> rawResultTask;
    private final ObjectMapper objectMapper;

    /**
     * Factory method to create a response instance.
     * 
     * @param response
     * @return a ready to use PBMapReduceResult
     */
    public static PBMapReduceResult newResult(final MapReduceResponseSource response) {
        final PBMapReduceResult result = new PBMapReduceResult(response);
        result.init();
        return result;
    }

    /**
     * runs the result task We need this so that we don't start a thread from
     * the constructor (escaping this problem)
     */
    private void init() {
        rawResultTask.run();
    }

    /**
     * @param response
     */
    private PBMapReduceResult(final MapReduceResponseSource response) {
        this.objectMapper = new ObjectMapper();

        // Getting the actual result from PB stream must be run once only
        rawResultTask = new FutureTask<String>(new Callable<String>() {

            public String call() throws Exception {
                JSONArray results = MapReduceResponseSource.readAllResults(response);
                return results.toString();
            }
        });
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.query.MapReduceResult#getResultRaw()
     */
    public String getResultRaw() {
        try {
            return rawResultTask.get();
        } catch (InterruptedException e) {
            // propagate it up
            Thread.currentThread().interrupt();
            // TODO, or return an empty result?
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.query.MapReduceResult#getResult(java.lang.Class)
     */
    public <T> Collection<T> getResult(Class<T> resultType) throws ConversionException {
        try {
            return objectMapper.readValue(getResultRaw(), objectMapper.getTypeFactory().constructCollectionType(Collection.class, resultType));
        } catch (IOException e) {
            throw new ConversionException(e);
        }
    }

}
