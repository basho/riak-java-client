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
import java.util.Date;
import java.util.Map.Entry;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;

import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.newapi.RiakObject;
import com.basho.riak.newapi.bucket.Bucket;
import com.basho.riak.newapi.bucket.BucketProperties;
import com.basho.riak.newapi.bucket.DefaultBucketProperties;
import com.basho.riak.newapi.builders.RiakObjectBuilder;
import com.basho.riak.newapi.cap.VClock;
import com.basho.riak.newapi.convert.ConversionException;
import com.basho.riak.newapi.query.MapReduceResult;
import com.basho.riak.pbc.MapReduceResponseSource;
import com.basho.riak.pbc.RequestMeta;
import com.basho.riak.pbc.mapreduce.MapReduceResponse;
import com.google.protobuf.ByteString;

/**
 * @author russell
 * 
 */
public class ConversionUtil {
    /**
     * @param fetch
     * @return
     */
    static RiakResponse convert(com.basho.riak.pbc.RiakObject[] pbcObjects, final Bucket bucket) {
        RiakResponse response = RiakResponse.empty();

        if (pbcObjects != null && pbcObjects.length > 0) {
            RiakObject[] converted = new RiakObject[pbcObjects.length];
            for (int i = 0; i < pbcObjects.length; i++) {
                converted[i] = convert(pbcObjects[i], bucket);
            }
            response = new RiakResponse(pbcObjects[0].getVclock().toByteArray(), converted);
        }

        return response;
    }

    /**
     * @param o
     * @return
     */
    static RiakObject convert(com.basho.riak.pbc.RiakObject o, final Bucket bucket) {
        RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(bucket, o.getKey());

        builder.withValue(nullSafeToStringUtf8(o.getValue()));
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
    static RequestMeta convert(StoreMeta storeMeta, RiakObject riakObject) {
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
     * Convert a {@link RiakObject} to a pbc
     * {@link com.basho.riak.pbc.RiakObject}
     * 
     * @param riakObject
     *            the RiakObject to convert
     * @return a {@link com.basho.riak.pbc.RiakObject} populated from riakObject
     */
    static com.basho.riak.pbc.RiakObject convert(RiakObject riakObject) {
        final VClock vc = riakObject.getVClock();
        ByteString bucketName = nullSafeToByteString(riakObject.getBucketName());
        ByteString key = nullSafeToByteString(riakObject.getKey());
        ByteString content = nullSafeToByteString(riakObject.getValue());

        ByteString vclock = null;
        if (vc != null) {
            vclock = nullSafeFromBytes(vc.getBytes());
        }

        com.basho.riak.pbc.RiakObject result = new com.basho.riak.pbc.RiakObject(vclock, bucketName, key, content);

        for (com.basho.riak.newapi.RiakLink link : riakObject) {
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
        return new DefaultBucketProperties.Builder().allowSiblings(properties.getAllowMult()).nVal(properties.getNValue()).build();
    }

    /**
     * @param resp
     * @return
     */
    static MapReduceResult convert(final MapReduceResponseSource resp) {
        final ObjectMapper om = new ObjectMapper();
        final StringBuilder sb = new StringBuilder();

        for (MapReduceResponse mrr : resp) {
            // TODO investigate pb client null returns from MRRS
            if (mrr != null && mrr.response != null) {
                sb.append(mrr.response.toStringUtf8());
            }
        }

        final MapReduceResult result = new MapReduceResult() {

            public String getResultRaw() {
                return sb.toString();
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
}
