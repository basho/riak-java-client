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

import static com.basho.riak.client.raw.pbc.ConversionUtil.convert;
import static com.basho.riak.client.raw.pbc.ConversionUtil.nullSafeToStringUtf8;

import java.io.IOException;
import java.util.Iterator;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;
import com.basho.riak.client.util.Constants;
import com.basho.riak.newapi.RiakObject;
import com.basho.riak.newapi.bucket.Bucket;
import com.basho.riak.newapi.bucket.BucketProperties;
import com.basho.riak.newapi.query.MapReduceResult;
import com.basho.riak.newapi.query.WalkResult;
import com.basho.riak.pbc.IRequestMeta;
import com.basho.riak.pbc.KeySource;
import com.basho.riak.pbc.MapReduceResponseSource;
import com.basho.riak.pbc.RequestMeta;
import com.basho.riak.pbc.RiakClient;
import com.google.protobuf.ByteString;

/**
 * Wraps the pb {@link RiakClient} and adapts it to the {@link RawClient} interface.
 * 
 * @author russell
 * 
 */
public class PBClientAdapter implements RawClient {

    private final RiakClient client;

    /**
     * @param client
     * @throws IOException
     */
    public PBClientAdapter(String host, int port) throws IOException {
        this.client = new RiakClient(host, port);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#fetch(java.lang.String,
     * java.lang.String)
     */
    public RiakResponse fetch(Bucket bucket, String key) throws IOException {
        if (bucket == null || bucket.getName() == null || bucket.getName().trim().equals("")) {
            throw new IllegalArgumentException(
                                               "bucket must not be null and bucket.getName() must not be null or empty "
                                                       + "or just whitespace.");
        }

        if (key == null || key.trim().equals("")) {
            throw new IllegalArgumentException("Key cannot be null or empty or just whitespace");
        }
        return convert(client.fetch(bucket.getName(), key), bucket);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#fetch(com.basho.riak.newapi.bucket
     * .Bucket, java.lang.String, int)
     */
    public RiakResponse fetch(Bucket bucket, String key, int readQuorum) throws IOException {
        if (bucket == null || bucket.getName() == null || bucket.getName().trim().equals("")) {
            throw new IllegalArgumentException(
                                               "bucket must not be null and bucket.getName() must not be null or empty "
                                                       + "or just whitespace.");
        }

        if (key == null || key.trim().equals("")) {
            throw new IllegalArgumentException("Key cannot be null or empty or just whitespace");
        }
        return convert(client.fetch(bucket.getName(), key, readQuorum), bucket);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.client.RiakObject
     * , com.basho.riak.client.raw.StoreMeta)
     */
    public RiakResponse store(RiakObject riakObject, StoreMeta storeMeta) throws IOException {
        if (riakObject == null || riakObject.getKey() == null || riakObject.getBucket() == null) {
            throw new IllegalArgumentException(
                                               "object cannot be null, object's key cannot be null, object's bucket cannot be null");
        }

        return convert(client.store(convert(riakObject), convert(storeMeta, riakObject)), riakObject.getBucket());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.client.RiakObject
     * )
     */
    public void store(RiakObject object) throws IOException {
        store(object, new StoreMeta(null, null, false));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String)
     */
    public void delete(Bucket bucket, String key) throws IOException {
        client.delete(bucket.getName(), key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String, int)
     */
    public void delete(Bucket bucket, String key, int deleteQuorum) throws IOException {
        client.delete(bucket.getName(), key, deleteQuorum);
    }

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

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#fetchBucketKeys(java.lang.String)
     */
    public Iterable<String> listKeys(String bucketName) throws IOException {
        if (bucketName == null || bucketName.trim().equals("")) {
            throw new IllegalArgumentException("bucketName cannot be null, empty or all whitespace");
        }

        final KeySource keySource = client.listKeys(ByteString.copyFromUtf8(bucketName));
        final Iterator<String> i = new Iterator<String>() {

            private final Iterator<ByteString> delegate = keySource.iterator();

            public boolean hasNext() {
                return delegate.hasNext();
            }

            public String next() {
                return nullSafeToStringUtf8(delegate.next());
            }

            public void remove() {
                delegate.remove();
            }
        };

        return new Iterable<String>() {
            public Iterator<String> iterator() {
                return i;
            }
        };
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
        IRequestMeta meta = new RequestMeta();
        meta.contentType(Constants.CTYPE_JSON);
        MapReduceResponseSource resp = client.mapReduce(spec.getJSON(), meta);
        return convert(resp);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#generateClientId()
     */
    public byte[] generateAndSetClientId() throws IOException {
        client.prepareClientID();
        return client.getClientID().getBytes();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#setClientId()
     */
    public void setClientId(byte[] clientId) throws IOException {
        if (clientId == null || clientId.length != 4) {
            throw new IllegalArgumentException("clientId must be 4 bytes. generateAndSetClientId() can do this for you");
        }
        client.setClientID(ByteString.copyFrom(clientId));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#getClientId()
     */
    public byte[] getClientId() throws IOException {
        final String clientId = client.getClientID();

        if (clientId != null) {
            return clientId.getBytes();
        } else {
            throw new IOException("null clientId returned by client");
        }
    }
}
