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
import static com.basho.riak.client.raw.pbc.ConversionUtil.linkAccumulateToLinkPhaseKeep;
import static com.basho.riak.client.raw.pbc.ConversionUtil.nullSafeToStringUtf8;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.query.BucketKeyMapReduce;
import com.basho.riak.client.query.LinkWalkStep;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.query.functions.JSSourceFunction;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.http.util.Constants;
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

    /**
     * Wrap a pre-created/configured pb client as a RawClient
     * @param delegate the {@link RiakClient} to adapt.
     */
    public PBClientAdapter(com.basho.riak.pbc.RiakClient delegate) {
        this.client = delegate;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#fetch(java.lang.String,
     * java.lang.String)
     */
    public RiakResponse fetch(String bucket, String key) throws IOException {
        if (bucket == null || bucket.trim().equals("")) {
            throw new IllegalArgumentException(
                                               "bucket must not be null or empty "
                                                       + "or just whitespace.");
        }

        if (key == null || key.trim().equals("")) {
            throw new IllegalArgumentException("Key cannot be null or empty or just whitespace");
        }
        return convert(client.fetch(bucket, key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#fetch(com.basho.riak.newapi.bucket
     * .Bucket, java.lang.String, int)
     */
    public RiakResponse fetch(String bucket, String key, int readQuorum) throws IOException {
        if (bucket == null || bucket.trim().equals("")) {
            throw new IllegalArgumentException(
                                               "bucket must not be null or empty "
                                                       + "or just whitespace.");
        }

        if (key == null || key.trim().equals("")) {
            throw new IllegalArgumentException("Key cannot be null or empty or just whitespace");
        }
        return convert(client.fetch(bucket, key, readQuorum));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.client.RiakObject
     * , com.basho.riak.client.raw.StoreMeta)
     */
    public RiakResponse store(IRiakObject riakObject, StoreMeta storeMeta) throws IOException {
        if (riakObject == null || riakObject.getKey() == null || riakObject.getBucket() == null) {
            throw new IllegalArgumentException(
                                               "object cannot be null, object's key cannot be null, object's bucket cannot be null");
        }

        return convert(client.store(convert(riakObject), convert(storeMeta, riakObject)));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.client.RiakObject
     * )
     */
    public void store(IRiakObject object) throws IOException {
        store(object, new StoreMeta(null, null, false));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String)
     */
    public void delete(String bucket, String key) throws IOException {
        client.delete(bucket, key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#delete(java.lang.String, int)
     */
    public void delete(String bucket, String key, int deleteQuorum) throws IOException {
        client.delete(bucket, key, deleteQuorum);
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

    /**
     * This is a bit of a hack. The pb interface doesn't really have a Link Walker
     * like the REST interface does. This method runs (maximum) 2 map reduce
     * requests to get the same results the link walk would for the given spec.
     * 
     * The first m/r job gets the end of the link walk and the inputs for second m/r job.
     * The second job gets all those inputs values.
     * Then some client side massaging occurs to massage the result into the correct format.
     */
    public WalkResult linkWalk(final LinkWalkSpec linkWalkSpec) throws IOException {
        MapReduceResult firstPhaseResult = linkWalkFirstPhase(linkWalkSpec);
        MapReduceResult secondPhaseResult = linkWalkSecondPhase(firstPhaseResult);
        WalkResult result = convert(secondPhaseResult);
        return result;
    }

    /**
     * Creates an m/r job from the supplied link spec and executes it
     *
     * @param linkWalkSpec
     *            the Link Walk spec
     * @return {@link MapReduceResult} containing the end of the link and any
     *         intermediate bkeys for a second pass
     * @throws IOException
     */
    private MapReduceResult linkWalkFirstPhase(final LinkWalkSpec linkWalkSpec) throws IOException {
        BucketKeyMapReduce mr = new BucketKeyMapReduce(this);
        mr.addInput(linkWalkSpec.getStartBucket(), linkWalkSpec.getStartKey());
        int size = linkWalkSpec.size();
        int cnt = 0;

        for (LinkWalkStep step : linkWalkSpec) {
            cnt++;
            boolean keep = linkAccumulateToLinkPhaseKeep(step.getKeep(), cnt == size);
            mr.addLinkPhase(step.getBucket(), step.getKey(), keep);
        }

        // this is a bit of a hack. The low level API is using the high level
        // API so must strip out the exception.
        try {
            return mr.execute();
        } catch (RiakException e) {
            throw (IOException) e.getCause();
        }
    }

    /**
     * Takes the results of running linkWalkFirstPhase and creates an m/r job
     * from them
     *
     * @param firstPhaseResult
     *            the results of running linkWalkfirstPhase.
     * @return the results from the intermediate bkeys of phase one.
     * @throws IOException
     */
    private MapReduceResult linkWalkSecondPhase(final MapReduceResult firstPhaseResult) throws IOException {
        try {
            @SuppressWarnings("rawtypes") Collection<LinkedList> bkeys = firstPhaseResult.getResult(LinkedList.class);

            BucketKeyMapReduce mr = new BucketKeyMapReduce(this);
            int stepCnt = 0;

            for (LinkedList<List<String>> step : bkeys) {
                // TODO find a way to *enforce* order here (custom
                // deserializer?)
                stepCnt++;
                for (List<String> input : step) {
                    // use the step count as key data so we can aggregate the
                    // results into the correct steps when they come back
                    mr.addInput(input.get(0), input.get(1), Integer.toString(stepCnt));
                }
            }

            mr.addReducePhase(new NamedErlangFunction("riak_kv_mapreduce", "reduce_set_union"), false);
            mr.addMapPhase(new JSSourceFunction("function(v, keyData) { return [{\"step\": keyData, \"v\": v}]; }"),
                           true);

            return mr.execute();
        } catch (ConversionException e) {
            throw new IOException(e.getMessage());
        } catch (RiakException e) {
            throw (IOException) e.getCause();
        }
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
