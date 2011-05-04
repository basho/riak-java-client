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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.httpclient.util.DateUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.http.RiakBucketInfo;
import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.request.RiakWalkSpec;
import com.basho.riak.client.http.response.BucketResponse;
import com.basho.riak.client.http.response.MapReduceResponse;
import com.basho.riak.client.http.response.WalkResponse;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.newapi.IRiakObject;
import com.basho.riak.newapi.bucket.BucketProperties;
import com.basho.riak.newapi.bucket.DefaultBucketProperties;
import com.basho.riak.newapi.builders.RiakObjectBuilder;
import com.basho.riak.newapi.convert.ConversionException;
import com.basho.riak.newapi.query.LinkWalkStep;
import com.basho.riak.newapi.query.MapReduceResult;
import com.basho.riak.newapi.query.WalkResult;
import com.basho.riak.newapi.query.functions.NamedErlangFunction;
import com.basho.riak.newapi.util.UnmodifiableIterator;

/**
 * @author russell
 * 
 */
public class ConversionUtil {
    /**
     * @param siblings
     * @param bucket
     * @return
     */
    static IRiakObject[] convert(Collection<com.basho.riak.client.http.RiakObject> siblings) {
        final Collection<IRiakObject> results = new ArrayList<IRiakObject>();

        for (com.basho.riak.client.http.RiakObject object : siblings) {
            results.add(convert(object));
        }

        return results.toArray(new IRiakObject[results.size()]);
    }

    /**
     * @param object
     * @return
     */
    static IRiakObject convert(final com.basho.riak.client.http.RiakObject o) {

        RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(o.getBucket(), o.getKey());

        builder.withValue(o.getValue());
        builder.withVClock(nullSafeGetBytes(o.getVclock()));
        builder.withVtag(o.getVtag());

        String lastModified = o.getLastmod();

        if (lastModified != null) {
            Date lastModDate = o.getLastmodAsDate();
            builder.withLastModified(lastModDate.getTime());
        }

        final Collection<RiakLink> links = new ArrayList<RiakLink>();

        for (com.basho.riak.client.http.RiakLink link : o.iterableLinks()) {
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
    static RiakLink convert(com.basho.riak.client.http.RiakLink link) {
        return new RiakLink(link.getBucket(), link.getKey(), link.getTag());
    }

    /**
     * @param vclock
     * @return
     */
    static byte[] nullSafeGetBytes(String vclock) {
        return vclock == null ? null : vclock.getBytes();
    }

    /**
     * @param storeMeta
     * @return
     */
    static RequestMeta convert(StoreMeta storeMeta) {
        RequestMeta requestMeta = RequestMeta.writeParams(storeMeta.getW(), storeMeta.getDw());

        if (storeMeta.hasReturnBody() && storeMeta.getReturnBody()) {
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
    static com.basho.riak.client.http.RiakObject convert(IRiakObject object, final RiakClient client) {
        com.basho.riak.client.http.RiakObject riakObject = new com.basho.riak.client.http.RiakObject(
                                                                                           client,
                                                                                           object.getBucket(),
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
    static String formatDate(Date lastModified) {
        if (lastModified == null) {
            return null;
        }
        return DateUtil.formatDate(lastModified);
    }

    /**
     * @param object
     * @return
     */
    static Map<String, String> getUserMetaData(IRiakObject object) {
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
    static List<com.basho.riak.client.http.RiakLink> getLinks(IRiakObject object) {

        final List<com.basho.riak.client.http.RiakLink> links = new ArrayList<com.basho.riak.client.http.RiakLink>();

        for (RiakLink link : object) {
            links.add(convert(link));
        }

        return links;
    }

    /**
     * @param link
     * @return
     */
    static com.basho.riak.client.http.RiakLink convert(RiakLink link) {
        return new com.basho.riak.client.http.RiakLink(link.getBucket(), link.getKey(), link.getTag());
    }

    /**
     * @param response
     * @return
     */
    static BucketProperties convert(BucketResponse response) {
        RiakBucketInfo bucketInfo = response.getBucketInfo();
        return new DefaultBucketProperties.Builder()
            .allowSiblings(bucketInfo.getAllowMult())
            .nVal(bucketInfo.getNVal())
            .chashKeyFunction(convert(bucketInfo.getCHashFun()))
            .linkWalkFunction(convert(bucketInfo.getLinkFun()))
            .build();
    }

    /**
     * @param cHashFun
     * @return
     */
    static NamedErlangFunction convert(String funString) {
        if (funString == null) {
            return null;
        }
        String[] fun = funString.split(":");

        if (fun.length != 2) {
            return null;
        }

        return new NamedErlangFunction(fun[0], fun[1]);
    }

    /**
     * @param bucketProperties
     * @return
     */
    static RiakBucketInfo convert(BucketProperties bucketProperties) {
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

    /**
     * @param resp
     * @return
     */
    static MapReduceResult convert(final MapReduceResponse resp) throws IOException {
        final ObjectMapper om = new ObjectMapper();

        final MapReduceResult result = new MapReduceResult() {

            public String getResultRaw() {
                return resp.getBodyAsString();
            }

            public <T> Collection<T> getResult(Class<T> resultType) throws ConversionException {
                try {
                    return om.readValue(getResultRaw(), TypeFactory.collectionType(Collection.class, resultType));
                } catch (IOException e) {
                    throw new ConversionException(e);
                }
            }
        };
        return result;
    }

    /**
     * @param linkWalkSpec
     * @return a String representation of this walk spec useful to the http.RiakClient
     */
    static String convert(LinkWalkSpec linkWalkSpec) {
        RiakWalkSpec riakWalkSpec = new RiakWalkSpec();
        for(LinkWalkStep step : linkWalkSpec) {
            riakWalkSpec.addStep(step.getBucket(), step.getKey(), step.getKeep().toString());
        }
        return riakWalkSpec.toString();
    }

    /**
     * Converts a WalkResponse -> WalkResult
     * @param walkResponse An http RiakClient WalkResponse
     * @return a new api WalkResult
     */
    static WalkResult convert(WalkResponse walkResponse) {
       final Collection<Collection<IRiakObject>> convertedSteps = new LinkedList<Collection<IRiakObject>>();

       for(List<com.basho.riak.client.http.RiakObject> step : walkResponse.getSteps()) {
            final LinkedList<IRiakObject> objects = new LinkedList<IRiakObject>();
            for(com.basho.riak.client.http.RiakObject o : step) {
                objects.add(convert(o));
            }
            convertedSteps.add(objects);
        }

       return new WalkResult() {
            public Iterator<Collection<IRiakObject>> iterator() {
                return  new UnmodifiableIterator<Collection<IRiakObject>>( convertedSteps.iterator() );
            }
        };
    }
}
