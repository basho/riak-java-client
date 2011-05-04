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
package com.basho.riak.client.raw;

import java.io.IOException;
import java.util.Iterator;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;

/**
 * @author russell
 * 
 */
public interface RawClient {

    // RiakObject

    RiakResponse fetch(String bucket, String key) throws IOException;

    RiakResponse fetch(String bucket, String key, int readQuorum) throws IOException;

    RiakResponse store(IRiakObject object, StoreMeta storeMeta) throws IOException;

    void store(IRiakObject object) throws IOException;

    void delete(String bucket, String key) throws IOException;

    void delete(String bucket, String key, int deleteQuorum) throws IOException;

    // Bucket
    Iterator<String> listBuckets() throws IOException;

    BucketProperties fetchBucket(String bucketName) throws IOException;

    void updateBucket(String name, BucketProperties bucketProperties) throws IOException;

    Iterable<String> listKeys(String bucketName) throws IOException;

    // Query
    WalkResult linkWalk(final LinkWalkSpec linkWalkSpec) throws IOException;

    MapReduceResult mapReduce(final MapReduceSpec spec) throws IOException, MapReduceTimeoutException;

    /**
     * If you don't set a client id explicitly at least call this to set one. It
     * generates the 4 byte ID and sets that Id on the client IE you *don't*
     * need to call setClientId() with the result of generate.
     * 
     * @return the generated clientId for the client
     */
    byte[] generateAndSetClientId() throws IOException;

    void setClientId(byte[] clientId) throws IOException;

    byte[] getClientId() throws IOException;
}
