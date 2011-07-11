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

import org.apache.http.impl.cookie.DateUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.http.RiakBucketInfo;
import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.query.LinkWalkStep;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.raw.JSONErrorParser;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;
import com.basho.riak.client.util.CharsetUtils;
import com.basho.riak.client.util.UnmodifiableIterator;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.request.RiakWalkSpec;
import com.basho.riak.client.http.response.BucketResponse;
import com.basho.riak.client.http.response.MapReduceResponse;
import com.basho.riak.client.http.response.WalkResponse;
import com.basho.riak.client.http.util.Constants;

/**
 * Static methods used internally by {@link HTTPClientAdapter} for converting
 * between http.{@link RiakClient} value classes and {@link RawClient} value
 * classes
 * 
 * @author russell
 * 
 */
public final class ConversionUtil {

    /**
     * All static methods so we don't want any instances created
     */
    private ConversionUtil() {}

    /**
     * Converts a Collection from legacy http {@link RiakObject} to
     * {@link IRiakObject}
     * 
     * @param siblings
     *            the siblings from the http client
     * @return an array of {@link IRiakObject}, one for each member of
     *         <code>siblings</code>
     */
    static IRiakObject[] convert(Collection<com.basho.riak.client.http.RiakObject> siblings) {
        final Collection<IRiakObject> results = new ArrayList<IRiakObject>();

        for (com.basho.riak.client.http.RiakObject object : siblings) {
            results.add(convert(object));
        }

        return results.toArray(new IRiakObject[results.size()]);
    }

    /**
     * Convert a {@link RiakObject} to an {@link IRiakObject}
     * 
     * @param object
     *            the {@link RiakObject} to convert
     * @return
     */
    static IRiakObject convert(final com.basho.riak.client.http.RiakObject o) {

        RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(o.getBucket(), o.getKey());

        builder.withValue(o.getValueAsBytes());
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
     * Convert a {@link com.basho.riak.client.http.RiakLink} to a
     * {@link RiakLink}
     * 
     * @param link
     *            the {@link com.basho.riak.client.http.RiakLink} to convert
     * @return a {@link RiakLink} with the same bucket/key/tag values
     */
    static RiakLink convert(com.basho.riak.client.http.RiakLink link) {
        return new RiakLink(link.getBucket(), link.getKey(), link.getTag());
    }

    /**
     * Get the <code>byte[]</code> of a vector clock string, in a null safe way.
     * @param vclock the String representation of a vector clock
     * @return <code>vclock</code> as an array of bytes or null (if <code>vclock</code> was null)
     */
    static byte[] nullSafeGetBytes(String vclock) {
        return vclock == null ? null : CharsetUtils.utf8StringToBytes(vclock);
    }

    /**
     * Convert a {@link StoreMeta} into a {@link RequestMeta} for use with the
     * legacy http client
     * 
     * @param storeMeta
     *            the {@link StoreMeta} to convert
     * @return a {@link RequestMeta} populated with <code>w/dw/returnBody</code>
     *         params from <code>storeMeta</code>
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
     * Convert an {@link IRiakObject} to an http.{@link RiakObject}, requires a {@link RiakClient}
     * @param object the {@link IRiakObject} to convert
     * @return a {@link RiakObject} populate with {@link IRiakObject}'s data
     */
    static com.basho.riak.client.http.RiakObject convert(IRiakObject object, final RiakClient client) {
        com.basho.riak.client.http.RiakObject riakObject = new com.basho.riak.client.http.RiakObject(
                                                                                           client,
                                                                                           object.getBucket(),
                                                                                           object.getKey(),
                                                                                           object.getValue(),
                                                                                           object.getContentType(),
                                                                                           getLinks(object),
                                                                                           getUserMetaData(object),
                                                                                           object.getVClockAsString(),
                                                                                           formatDate(object.getLastModified()),
                                                                                           object.getVtag());
        return riakObject;
    }

    /**
     * {@link RiakObject} expects the date as a string in a certain format
     * @param lastModified the date to format
     * @return null (if <code>lastModified</code> was null) or a String of the date
     */
    static String formatDate(Date lastModified) {
        if (lastModified == null) {
            return null;
        }
        return DateUtils.formatDate(lastModified);
    }

    /**
     * Copies the user meta data from an {@link IRiakObject} into a {@link Map}
     * @param object the {@link IRiakObject} whose meta data we want
     * @return the map of user meta (may be empty, won't be null)
     */
    static Map<String, String> getUserMetaData(IRiakObject object) {
        final Map<String, String> userMetaData = new HashMap<String, String>();

        for (Entry<String, String> entry : object.userMetaEntries()) {
            userMetaData.put(entry.getKey(), entry.getValue());
        }
        return userMetaData;
    }

    /**
     * Copy the {@link RiakLink}s from an {@link IRiakObject} into a
     * {@link List}
     * 
     * @param object
     *            the {@link IRiakObject}s whose links we want
     * @return a {@link List} of {@link com.basho.riak.client.http.RiakLink},
     *         maybe empty, won't be null
     */
    static List<com.basho.riak.client.http.RiakLink> getLinks(IRiakObject object) {

        final List<com.basho.riak.client.http.RiakLink> links = new ArrayList<com.basho.riak.client.http.RiakLink>();

        for (RiakLink link : object) {
            links.add(convert(link));
        }

        return links;
    }

    /**
     * Convert an http {@link com.basho.riak.client.http.RiakLink} to a
     * {@link RiakLink}
     * 
     * @param link
     *            the {@link com.basho.riak.client.http.RiakLink} to convert
     * @return a {@link RiakLink} with the same <code>bucket/key/tag</code>
     *         data.
     */
    static com.basho.riak.client.http.RiakLink convert(RiakLink link) {
        return new com.basho.riak.client.http.RiakLink(link.getBucket(), link.getKey(), link.getTag());
    }

    /**
     * Copy the data from a {@link BucketResponse} into a
     * {@link BucketProperties}
     * 
     * @param response
     *            the {@link BucketResponse} to copy
     * @return a {@link BucketProperties} populated from <code>response</code>
     */
    static BucketProperties convert(BucketResponse response) {
        RiakBucketInfo bucketInfo = response.getBucketInfo();
        return new BucketPropertiesBuilder()
            .allowSiblings(bucketInfo.getAllowMult())
            .nVal(bucketInfo.getNVal())
            .chashKeyFunction(convert(bucketInfo.getCHashFun()))
            .linkWalkFunction(convert(bucketInfo.getLinkFun()))
            .build();
    }

    /**
     * Parse a http client chash_fun string into a {@link NamedErlangFunction}
     * 
     * @param funString
     *            a String of the format "mod:fun"
     * @return a {@link NamedErlangFunction} populated from
     *         <code>funString</code> or <code>null</code> if the string cannot
     *         be parsed.
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
     * Turn a {@link BucketProperties} into a {@link RiakBucketInfo} for
     * persisting.
     * 
     * @param bucketProperties
     *            the {@link BucketProperties} to convert
     * @return a {@link RiakBucketInfo} populated from the
     *         {@link BucketProperties}
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
     * Turn a {@link MapReduceResponse} into a {@link MapReduceResult} Creates
     * an anonymous inner class implementation of {@link MapReduceResult} and
     * uses Jackson's ObjectMapper to convert the response payload.
     * 
     * @param resp
     *            the {@link MapReduceResponse}
     * @return a {@link MapReduceResult} view of the results in
     *         <code>resp</code>
     * @throws MapReduceTimeoutException
     */
    static MapReduceResult convert(final MapReduceResponse resp) throws IOException, MapReduceTimeoutException {
        if(resp.isError()) {
            if(JSONErrorParser.isTimeoutException(resp.getBodyAsString())) {
                throw new MapReduceTimeoutException();
            } else {
                throw new IOException(resp.getBodyAsString());
            }
        }
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
     * Convert a {@link LinkWalkSpec} to a String for execution by the http.
     * {@link RiakClient}
     * 
     * @param linkWalkSpec
     *            the {@link LinkWalkSpec}
     * @return a String representation of <code>linkWalkSpec</code> useful to
     *         the http.{@link RiakClient}
     */
    static String convert(LinkWalkSpec linkWalkSpec) {
        RiakWalkSpec riakWalkSpec = new RiakWalkSpec();
        for(LinkWalkStep step : linkWalkSpec) {
            riakWalkSpec.addStep(step.getBucket(), step.getTag(), step.getKeep().toString());
        }
        return riakWalkSpec.toString();
    }

    /**
     * Converts a {@link WalkResponse} -> {@link WalkResult}
     * 
     * Creates an anonymous implementation of {@link WalkResult} that exposes an
     * {@link UnmodifiableIterator} view of the results.
     * 
     * @param walkResponse
     *            An http.{@link RiakClient} {@link WalkResponse}
     * @return a new {@link WalkResult}
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
