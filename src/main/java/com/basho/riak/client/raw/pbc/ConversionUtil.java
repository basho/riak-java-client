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

import static com.basho.riak.client.util.CharsetUtils.asBytes;
import static com.basho.riak.client.util.CharsetUtils.getCharset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.http.impl.cookie.DateParseException;
import org.apache.http.impl.cookie.DateUtils;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.query.LinkWalkStep.Accumulate;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.util.CharsetUtils;
import com.basho.riak.client.util.UnmodifiableIterator;
import com.basho.riak.pbc.MapReduceResponseSource;
import com.basho.riak.pbc.RequestMeta;
import com.google.protobuf.ByteString;

/**
 * 
 * @author russell
 * 
 */
public final class ConversionUtil {

    /**
     * All static methods, don't allow any instances to be created.
     */
    private ConversionUtil() {}

    /**
     * @param fetch
     * @return
     */
    static RiakResponse convert(com.basho.riak.pbc.RiakObject[] pbcObjects) {
        RiakResponse response = RiakResponse.empty();

        if (pbcObjects != null && pbcObjects.length > 0) {
            IRiakObject[] converted = new IRiakObject[pbcObjects.length];
            for (int i = 0; i < pbcObjects.length; i++) {
                converted[i] = convert(pbcObjects[i]);
            }
            response = new RiakResponse(pbcObjects[0].getVclock().toByteArray(), converted);
        }

        return response;
    }

    /**
     * @param o
     * @return
     */
    static IRiakObject convert(com.basho.riak.pbc.RiakObject o) {
        RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(o.getBucket(), o.getKey());

        builder.withValue(nullSafeToBytes(o.getValue()));
        builder.withVClock(nullSafeToBytes(o.getVclock()));
        builder.withVtag(o.getVtag());

        Date lastModified = o.getLastModified();

        if (lastModified != null) {
            builder.withLastModified(lastModified.getTime());
        }

        return builder.build();
    }

    /**
     * @param vclock
     * @return
     */
    static byte[] nullSafeToBytes(ByteString value) {
        return value == null ? null : value.toByteArray();
    }

    /**
     * @param value
     * @return
     */
    static String nullSafeToStringUtf8(ByteString value) {
        return value == null ? null : value.toStringUtf8();
    }

    static ByteString nullSafeToByteString(String value) {
        return value == null ? null : ByteString.copyFromUtf8(value);
    }

    /**
     * Convert a {@link StoreMeta} to a pbc {@link RequestMeta}
     * 
     * @param storeMeta
     *            a {@link StoreMeta} for the store operation.
     * @return a {@link RequestMeta} populated from the storeMeta's values.
     */
    static RequestMeta convert(StoreMeta storeMeta, IRiakObject riakObject) {
        RequestMeta requestMeta = new RequestMeta();
        if (storeMeta.hasW()) {
            requestMeta.w(storeMeta.getW());
        }
        if (storeMeta.hasDW()) {
            requestMeta.dw(storeMeta.getDw());
        }
        if (storeMeta.hasReturnBody()) {
            requestMeta.returnBody(storeMeta.getReturnBody());
        }
        String contentType = riakObject.getContentType();
        if (contentType != null) {
            requestMeta.contentType(contentType);
        }
        return requestMeta;
    }

    /**
     * Convert a {@link IRiakObject} to a pbc
     * {@link com.basho.riak.pbc.RiakObject}
     * 
     * @param riakObject
     *            the RiakObject to convert
     * @return a {@link com.basho.riak.pbc.RiakObject} populated from riakObject
     */
    static com.basho.riak.pbc.RiakObject convert(IRiakObject riakObject) {
        final VClock vc = riakObject.getVClock();
        ByteString bucketName = nullSafeToByteString(riakObject.getBucket());
        ByteString key = nullSafeToByteString(riakObject.getKey());
        ByteString content = ByteString.copyFrom(riakObject.getValue());

        ByteString vclock = null;
        if (vc != null) {
            vclock = nullSafeFromBytes(vc.getBytes());
        }

        com.basho.riak.pbc.RiakObject result = new com.basho.riak.pbc.RiakObject(vclock, bucketName, key, content);

        for (com.basho.riak.client.RiakLink link : riakObject) {
            result.addLink(link.getTag(), link.getBucket(), link.getKey());
        }

        for (Entry<String, String> metaDataItem : riakObject.userMetaEntries()) {
            result.addUsermetaItem(metaDataItem.getKey(), metaDataItem.getValue());
        }

        result.setContentType(riakObject.getContentType());
        return result;
    }

    /**
     * @param bytes
     * @return
     */
    static ByteString nullSafeFromBytes(byte[] bytes) {
        return ByteString.copyFrom(bytes);
    }

    /**
     * @param bucketProperties
     * @return
     */
    static com.basho.riak.pbc.BucketProperties convert(BucketProperties p) {
        return new com.basho.riak.pbc.BucketProperties().nValue(p.getNVal()).allowMult(p.getAllowSiblings());
    }

    /**
     * @param properties
     * @return
     */
    static BucketProperties convert(com.basho.riak.pbc.BucketProperties properties) {
        return new BucketPropertiesBuilder().allowSiblings(properties.getAllowMult()).nVal(properties.getNValue()).build();
    }

    /**
     * @param response
     * @return
     * @throws IOException
     */
    static MapReduceResult convert(final MapReduceResponseSource response) throws IOException {
        return PBMapReduceResult.newResult(response);
    }

    /**
     * Converts an Http {@link Accumulate} value into a boolean
     *
     * @param accumulate
     *            the {@link Accumulate} value
     * @param isFinalStep
     *            is the {@link Accumulate} value for the final step
     * @return true if a m/r link phase should keep the result or false
     *         otherwise
     */
    static boolean linkAccumulateToLinkPhaseKeep(Accumulate accumulate, boolean isFinalStep) {
        // (in m/r terms we *always* want to keep the final step since its
        // output
        // is the input to the final map stage which we *do* keep)
        boolean keep = true;
        if (!isFinalStep) {
            switch (accumulate) {
            case YES:
                keep = true;
                break;
            case NO:
            case DEFAULT:
                keep = false;
                break;
            default:
                break;
            }
        }
        return keep;
    }

    /**
     * Take a link walked m/r result and make it into a WalkResult.
     *
     * This is a little bit nasty since the JSON is parsed to a Map.
     *
     * @param secondPhaseResult
     *            the contents of which *must* be a json array of {step: int, v:
     *            riakObjectMap}
     * @return a WalkResult of RiakObjects grouped by first-phase step
     * @throws IOException
     */
    @SuppressWarnings({ "rawtypes" }) static WalkResult convert(MapReduceResult secondPhaseResult) throws IOException {
        final SortedMap<Integer, Collection<IRiakObject>> steps = new TreeMap<Integer, Collection<IRiakObject>>();

        try {
            Collection<Map> results = secondPhaseResult.getResult(Map.class);
            for (Map o : results) {
                final int step = Integer.parseInt((String) o.get("step"));
                Collection<IRiakObject> stepAccumulator = steps.get(step);

                if (stepAccumulator == null) {
                    stepAccumulator = new ArrayList<IRiakObject>();
                    steps.put(step, stepAccumulator);
                }

                final Map data = (Map) o.get("v");

                stepAccumulator.add(mapToRiakObject(data));
            }
        } catch (ConversionException e) {
            throw new IOException(e.getMessage());
        }
        // create a result instance
        return new WalkResult() {
            public Iterator<Collection<IRiakObject>> iterator() {
                return new UnmodifiableIterator<Collection<IRiakObject>>(steps.values().iterator());
            }
        };
    }

    /**
     * Copy the data from the map into a RiakObject.
     *
     * @param data
     *            a valid Map from JSON.
     * @return A RiakObject populated from the map.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" }) private static IRiakObject mapToRiakObject(Map data) {
        RiakObjectBuilder b = RiakObjectBuilder.newBuilder((String) data.get("bucket"), (String) data.get("key"));
        String vclock = (String) data.get("vclock");
        b.withVClock(CharsetUtils.utf8StringToBytes(vclock));

        final List values = (List) data.get("values");
        // TODO figure out what to do about multiple values here,
        // I say take the first for now (that is what the link walk interface
        // does)
        if (values.size() != 0) {
            final Map value = (Map) values.get(0);
            final Map meta = (Map) value.get("metadata");
            final String contentType = (String) meta.get("content-type");

            b.withValue(asBytes((String) value.get("data"), getCharset(contentType)));
            b.withContentType(contentType);
            b.withVtag((String) meta.get("X-Riak-VTag"));

            try {
                Date lastModDate = DateUtils.parseDate((String) meta.get("X-Riak-Last-Modified"));
                b.withLastModified(lastModDate.getTime());
            } catch (DateParseException e) {
                // NO-OP
            }

            List<List<String>> links = (List<List<String>>) meta.get("Links");
            for (List<String> link : links) {
                b.addLink(link.get(0), link.get(1), link.get(2));
            }

            Map<String, String> userMetaData = (Map<String, String>) meta.get("X-Riak-Meta");
            b.withUsermeta(userMetaData);
        }
        return b.build();
    }
}