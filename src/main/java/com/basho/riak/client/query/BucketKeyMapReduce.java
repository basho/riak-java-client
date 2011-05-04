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
package com.basho.riak.client.query;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.codehaus.jackson.JsonGenerator;

import com.basho.riak.client.raw.RawClient;

/**
 * @author russell
 * 
 */

public class BucketKeyMapReduce extends MapReduce implements Iterable<String[]> {

    private final Object inputsLock = new Object();
    private final Collection<String[]> inputs = new LinkedList<String[]>();

    /**
     * @param client
     */
    public BucketKeyMapReduce(RawClient client) {
        super(client);
    }

    /**
     * Add a bucket, key, keydata to the list of inputs for the m/r query
     * 
     * @param bucket
     * @param key
     * @param keyData
     * @return this
     */
    public BucketKeyMapReduce addInput(String bucket, String key, String keyData) {
        synchronized (inputsLock) {
            inputs.add(new String[] {bucket, key, keyData});
        }

        return this;
    }

    /**
     * Add a bucket, key input to the query
     * 
     * @param bucket
     * @param key
     * @return
     */
    public BucketKeyMapReduce addInput(String bucket, String key) {
        synchronized (inputsLock) {
            inputs.add(new String[] {bucket, key});
        }

        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Iterable#iterator()
     */
    public Iterator<String[]> iterator() {
        final Collection<String[]> inputsCopy = new LinkedList<String[]>();

        synchronized (inputsLock) {
            inputsCopy.addAll(inputs);
        }

        return inputsCopy.iterator();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.newapi.query.MapReduce#writeInput(org.codehaus.jackson.JsonGenerator)
     */
    @Override protected void writeInput(JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeObject(this);
    }
}
