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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;
import com.basho.riak.newapi.RiakObject;
import com.basho.riak.newapi.bucket.Bucket;
import com.basho.riak.newapi.bucket.BucketProperties;
import com.basho.riak.newapi.bucket.DefaultBucketProperties;
import com.basho.riak.newapi.builders.RiakObjectBuilder;
import com.basho.riak.newapi.query.MapReduceResult;
import com.basho.riak.newapi.query.MapReduceSpec;
import com.basho.riak.newapi.query.WalkResult;
import com.basho.riak.pbc.RiakClient;
import com.google.protobuf.ByteString;

/**
 * @author russell
 * 
 */
public class PBClient implements RawClient {

    private final RiakClient client;

    /**
     * @param client
     * @throws IOException
     */
    public PBClient(String host, int port) throws IOException {
        this.client = new RiakClient(host, port);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#fetch(java.lang.String,
     * java.lang.String)
     */
    public RiakObject[] fetch(Bucket bucket, String key) throws IOException {
        if (bucket == null || bucket.getName() == null || bucket.getName().trim().equals("")) {
            throw new IllegalArgumentException(
                                               "bucket must not be null and bucket.getName() must not be null or empty or just whitespace.");
        }

        if (key == null || key.trim().equals("")) {
            throw new IllegalArgumentException("Key cannot be null or empty or just whitespace");
        }
        return convert(client.fetch(bucket.getName(), key), bucket);
    }

    /**
     * @param fetch
     * @return
     */
    private RiakObject[] convert(com.basho.riak.pbc.RiakObject[] pbcObjects, final Bucket bucket) {
        Collection<RiakObject> converted = new ArrayList<RiakObject>();

        if (pbcObjects != null) {
            for (com.basho.riak.pbc.RiakObject o : pbcObjects) {
                converted.add(convert(o, bucket));
            }
        }

        return converted.toArray(new RiakObject[converted.size()]);
    }

    /**
     * @param o
     * @return
     */
    private RiakObject convert(com.basho.riak.pbc.RiakObject o, final Bucket bucket) {
        RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(bucket, o.getKey());

        builder.withValue(nullSafeToStringUtf8(o.getValue()));
        builder.withVClock(nullSafeToStringUtf8(o.getVclock()));

        Date lastModified = o.getLastModified();

        if (lastModified != null) {
            builder.withLastModified(lastModified.getTime());
        }

        return builder.build();
    }

    /**
     * @param value
     * @return
     */
    private String nullSafeToStringUtf8(ByteString value) {
        return value == null ? null : value.toStringUtf8();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.client.RiakObject
     * , com.basho.riak.client.raw.StoreMeta)
     */
    public RiakObject store(RiakObject object, StoreMeta storeMeta) throws IOException {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.client.RiakObject
     * )
     */
    public void store(RiakObject object) throws IOException {}

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#delete(com.basho.riak.client.RiakObject
     * )
     */
    public void delete(RiakObject object) throws IOException {}

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#listBuckets()
     */
    public Iterator<String> listBuckets() throws IOException {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#fetchBucket(java.lang.String)
     */
    public BucketProperties fetchBucket(String bucketName) throws IOException {
        if (bucketName == null || bucketName.trim().equals("")) {
            throw new IllegalArgumentException("bucketName cannot be null, empty or all whitespace");
        }
        com.basho.riak.pbc.BucketProperties properties = client.getBucketProperties(ByteString.copyFromUtf8(bucketName));

        return convert(properties);
    }

    /**
     * @param properties
     * @return
     */
    private BucketProperties convert(com.basho.riak.pbc.BucketProperties properties) {
        return new DefaultBucketProperties.Builder().allowSiblings(properties.getAllowMult()).nVal(properties.getNValue()).build();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#updateBucketProperties(com.basho.
     * riak.client.bucket.BucketProperties)
     */
    public void updateBucket(final String name, final BucketProperties bucketProperties) throws IOException {
        com.basho.riak.pbc.BucketProperties properties = convert(bucketProperties);
        client.setBucketProperties(ByteString.copyFromUtf8(name), properties);

    }

    /**
     * @param bucketProperties
     * @return
     */
    private com.basho.riak.pbc.BucketProperties convert(BucketProperties p) {
        return new com.basho.riak.pbc.BucketProperties().nValue(p.getNVal()).allowMult(p.getAllowSiblings());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#fetchBucketKeys(java.lang.String)
     */
    public Iterator<String> fetchBucketKeys(String bucketName) throws IOException {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#linkWalk(com.basho.riak.client.RiakObject
     * , com.basho.riak.client.raw.query.LinkWalkSpec)
     */
    public WalkResult linkWalk(RiakObject startObject, LinkWalkSpec linkWalkSpec) throws IOException {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#mapReduce(com.basho.riak.client.query
     * .MapReduceSpec)
     */
    public MapReduceResult mapReduce(MapReduceSpec spec) throws IOException, MapReduceTimeoutException {
        return null;
    }

}
