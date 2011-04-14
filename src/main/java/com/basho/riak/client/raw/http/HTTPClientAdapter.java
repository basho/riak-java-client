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
package com.basho.riak.client.raw.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.httpclient.util.DateUtil;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.response.WithBodyResponse;
import com.basho.riak.client.util.Constants;
import com.basho.riak.newapi.DefaultRiakLink;
import com.basho.riak.newapi.RiakLink;
import com.basho.riak.newapi.RiakObject;
import com.basho.riak.newapi.bucket.Bucket;
import com.basho.riak.newapi.bucket.BucketProperties;
import com.basho.riak.newapi.bucket.DefaultBucketProperties;
import com.basho.riak.newapi.builders.RiakObjectBuilder;
import com.basho.riak.newapi.cap.ClientId;
import com.basho.riak.newapi.query.MapReduceResult;
import com.basho.riak.newapi.query.MapReduceSpec;
import com.basho.riak.newapi.query.NamedErlangFunction;
import com.basho.riak.newapi.query.WalkResult;

/**
 * @author russell
 * 
 */
public class HTTPClientAdapter implements RawClient {

    private final RiakClient client;

    /**
     * @param client
     */
    public HTTPClientAdapter(RiakClient client) {
        this.client = client;
    }

    /**
     * @param string
     * @param i
     */
    public HTTPClientAdapter(String url) {
        this(new RiakClient(url));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#fetch(com.basho.riak.newapi.bucket
     * .Bucket, java.lang.String)
     */
    public RiakObject[] fetch(Bucket bucket, String key) throws IOException {
        if (bucket == null || bucket.getName() == null || bucket.getName().trim().equals("")) {
            throw new IllegalArgumentException(
                                               "bucket must not be null and bucket.getName() must not be null or empty "
                                                       + "or just whitespace.");
        }

        if (key == null || key.trim().equals("")) {
            throw new IllegalArgumentException("Key cannot be null or empty or just whitespace");
        }

        FetchResponse resp = client.fetch(bucket.getName(), key);

        return handleBodyResponse(bucket, resp);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#fetch(com.basho.riak.newapi.bucket
     * .Bucket, java.lang.String, int)
     */
    public RiakObject[] fetch(Bucket bucket, String key, int readQuorum) throws IOException {
        if (bucket == null || bucket.getName() == null || bucket.getName().trim().equals("")) {
            throw new IllegalArgumentException(
                                               "bucket must not be null and bucket.getName() must not be null or empty "
                                                       + "or just whitespace.");
        }

        if (key == null || key.trim().equals("")) {
            throw new IllegalArgumentException("Key cannot be null or empty or just whitespace");
        }

        FetchResponse resp = client.fetch(bucket.getName(), key, RequestMeta.readParams(readQuorum));

        return handleBodyResponse(bucket, resp);
    }

    /**
     * @param bucket
     * @param resp
     * @return
     */
    private RiakObject[] handleBodyResponse(Bucket bucket, WithBodyResponse resp) {
        if (resp.hasSiblings()) {
            return convert(resp.getSiblings(), bucket);
        } else if (resp.hasObject()) {
            return new RiakObject[] { convert(resp.getObject(), bucket) };
        } else  {
            return new RiakObject[] {};
        }
    }

    /**
     * @param siblings
     * @param bucket
     * @return
     */
    private RiakObject[] convert(Collection<com.basho.riak.client.RiakObject> siblings, Bucket bucket) {
        final Collection<RiakObject> results = new ArrayList<RiakObject>();

        for (com.basho.riak.client.RiakObject object : siblings) {
            results.add(convert(object, bucket));
        }

        return results.toArray(new RiakObject[results.size()]);
    }

    /**
     * @param object
     * @return
     */
    private RiakObject convert(final com.basho.riak.client.RiakObject o, final Bucket bucket) {

        RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(bucket, o.getKey());

        builder.withValue(o.getValue());
        builder.withVClock(nullSafeGetBytes(o.getVclock()));
        builder.withVtag(o.getVtag());

        String lastModified = o.getLastmod();

        if (lastModified != null) {
            Date lastModDate = o.getLastmodAsDate();
            builder.withLastModified(lastModDate.getTime());
        }

        final Collection<RiakLink> links = new ArrayList<RiakLink>();

        for (com.basho.riak.client.RiakLink link : o.iterableLinks()) {
            links.add(convert(link));
        }

        builder.withLinks(links);
        builder.withContentType(o.getContentType());

        final Map<String, String> userMetaData = new HashMap<String, String>();

        for (String key : o.usermetaKeys()) {
            userMetaData.put(key, o.getUsermetaItem(key));
        }

        builder.withUsermeta(userMetaData);

        return builder.build();
    }

    /**
     * @param link
     * @return
     */
    private RiakLink convert(com.basho.riak.client.RiakLink link) {
        return new DefaultRiakLink(link.getBucket(), link.getKey(), link.getTag());
    }

    /**
     * @param vclock
     * @return
     */
    private byte[] nullSafeGetBytes(String vclock) {
        return vclock == null ? null : vclock.getBytes();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.newapi.RiakObject
     * , com.basho.riak.client.raw.StoreMeta)
     */
    public RiakObject[] store(RiakObject object, StoreMeta storeMeta) throws IOException {
        if(object == null || object.getBucket() == null) {
            throw new IllegalArgumentException("cannot store a null RiakObject, or a RiakObject without a bucket");
        }
        final Bucket bucket = object.getBucket();
        
        RiakObject[] result = new RiakObject[] {};
        
        com.basho.riak.client.RiakObject riakObject = convert(object);
        RequestMeta requestMeta = convert(storeMeta);
        StoreResponse resp = client.store(riakObject, requestMeta);
        
        if(resp.isSuccess()) {
            riakObject.updateMeta(resp);
        } else {
            throw new IOException(resp.getBodyAsString());
        }
        
        if(storeMeta.hasReturnBody() && storeMeta.getReturnBody()) {
           result = handleBodyResponse(bucket, resp);
        } 

        return result;
    }

    /**
     * @param storeMeta
     * @return
     */
    private RequestMeta convert(StoreMeta storeMeta) {
        RequestMeta requestMeta = RequestMeta.writeParams(storeMeta.getW(), storeMeta.getDw());
        
        if(storeMeta.hasReturnBody() && storeMeta.getReturnBody()) {
            requestMeta.setQueryParam(Constants.QP_RETURN_BODY, Boolean.toString(true));
        } else {
            requestMeta.setQueryParam(Constants.QP_RETURN_BODY, Boolean.toString(false));
        }
        
        return requestMeta;
    }

    /**
     * @param object
     * @return
     */
    private com.basho.riak.client.RiakObject convert(RiakObject object) {

        com.basho.riak.client.RiakObject riakObject = new com.basho.riak.client.RiakObject(
                                                                                           client,
                                                                                           object.getBucketName(),
                                                                                           object.getKey(),
                                                                                           nullSafeGetBytes(object.getValue()),
                                                                                           object.getContentType(),
                                                                                           getLinks(object),
                                                                                           getUserMetaData(object),
                                                                                           object.getVClockAsString(),
                                                                                           formatDate(object.getLastModified()),
                                                                                           object.getVtag());
        return riakObject;
    }

    /**
     * @param lastModified
     * @return
     */
    private String formatDate(Date lastModified) {
        if(lastModified == null) {
            return null;
        }
        return DateUtil.formatDate(lastModified);
    }

    /**
     * @param object
     * @return
     */
    private Map<String, String> getUserMetaData(RiakObject object) {
        final Map<String, String> userMetaData = new HashMap<String, String>();

        for (Entry<String, String> entry : object.userMetaEntries()) {
            userMetaData.put(entry.getKey(), entry.getValue());
        }
        return userMetaData;
    }

    /**
     * @param object
     * @return
     */
    private List<com.basho.riak.client.RiakLink> getLinks(RiakObject object) {

        final List<com.basho.riak.client.RiakLink> links = new ArrayList<com.basho.riak.client.RiakLink>();

        for (RiakLink link : object) {
            links.add(convert(link));
        }

        return links;
    }

    /**
     * @param link
     * @return
     */
    private com.basho.riak.client.RiakLink convert(RiakLink link) {
        return new com.basho.riak.client.RiakLink(link.getBucket(), link.getKey(), link.getTag());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.newapi.RiakObject
     * )
     */
    public void store(RiakObject object) throws IOException {
        store(object, new StoreMeta(null, null, false));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#delete(com.basho.riak.newapi.bucket
     * .Bucket, java.lang.String)
     */
    public void delete(Bucket bucket, String key) throws IOException {
       HttpResponse resp = client.delete(bucket.getName(), key);
       if(!resp.isSuccess()) {
           throw new IOException(resp.getBodyAsString());
       }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#delete(com.basho.riak.newapi.bucket
     * .Bucket, java.lang.String, int)
     */
    public void delete(Bucket bucket, String key, int deleteQuorum) throws IOException {
        HttpResponse resp = client.delete(bucket.getName(), key, RequestMeta.deleteParams(deleteQuorum));
        if(!resp.isSuccess()) {
            throw new IOException(resp.getBodyAsString());
        }
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

        BucketResponse response = client.getBucketSchema(bucketName, null);

        return convert(response);
    }

    /**
     * @param response
     * @return
     */
    private BucketProperties convert(BucketResponse response) {
        RiakBucketInfo bucketInfo = response.getBucketInfo();
        return new DefaultBucketProperties.Builder().allowSiblings(bucketInfo.getAllowMult()).nVal(bucketInfo.getNVal()).chashKeyFunction(convert(bucketInfo.getCHashFun())).linkWalkFunction(convert(bucketInfo.getLinkFun())).build();
    }

    /**
     * @param cHashFun
     * @return
     */
    private NamedErlangFunction convert(String funString) {
        if (funString == null) {
            return null;
        }
        String[] fun = funString.split(":");

        if (fun.length != 2) {
            return null;
        }

        return new NamedErlangFunction(fun[0], fun[1]);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#updateBucket(java.lang.String,
     * com.basho.riak.newapi.bucket.BucketProperties)
     */
    public void updateBucket(String name, BucketProperties bucketProperties) throws IOException {
        HttpResponse response = client.setBucketSchema(name, convert(bucketProperties));
        if (!response.isSuccess()) {
            throw new IOException(response.getBodyAsString());
        }

    }

    /**
     * @param bucketProperties
     * @return
     */
    private RiakBucketInfo convert(BucketProperties bucketProperties) {
        RiakBucketInfo rbi = new RiakBucketInfo();

        if (bucketProperties.getAllowSiblings() != null) {
            rbi.setAllowMult(bucketProperties.getAllowSiblings());
        }

        if (bucketProperties.getNVal() != null) {
            rbi.setNVal(bucketProperties.getNVal());
        }

        final NamedErlangFunction chashKeyFun = bucketProperties.getChashKeyFunction();
        if (chashKeyFun != null) {
            rbi.setCHashFun(chashKeyFun.getMod(), chashKeyFun.getFun());
        }

        final NamedErlangFunction linkwalkFun = bucketProperties.getLinkWalkFunction();
        if (linkwalkFun != null) {
            rbi.setLinkFun(linkwalkFun.getMod(), linkwalkFun.getFun());
        }

        return rbi;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#fetchBucketKeys(java.lang.String)
     */
    public Iterable<String> listKeys(String bucketName) throws IOException {
        final BucketResponse bucketResponse = client.streamBucket(bucketName);
        final KeySource keyStream = new KeySource(bucketResponse);
        return new Iterable<String>() {
            public Iterator<String> iterator() {
                return keyStream;
            }
        };
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#linkWalk(com.basho.riak.newapi.RiakObject
     * , com.basho.riak.client.raw.query.LinkWalkSpec)
     */
    public WalkResult linkWalk(RiakObject startObject, LinkWalkSpec linkWalkSpec) throws IOException {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#mapReduce(com.basho.riak.newapi.query
     * .MapReduceSpec)
     */
    public MapReduceResult mapReduce(MapReduceSpec spec) throws IOException, MapReduceTimeoutException {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#generateAndSetClientId()
     */
    public byte[] generateAndSetClientId() throws IOException {
        byte[] clientId = ClientId.generate();

        client.setClientId(new String(clientId));
        return client.getClientId();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#setClientId(byte[])
     */
    public void setClientId(byte[] clientId) throws IOException {
        if (clientId == null || clientId.length != 4) {
            throw new IllegalArgumentException("clientId must be 4 bytes. generateAndSetClientId() can do this for you");
        }
        client.setClientId(new String(clientId));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#getClientId()
     */
    public byte[] getClientId() throws IOException {
        return client.getClientId();
    }

}
