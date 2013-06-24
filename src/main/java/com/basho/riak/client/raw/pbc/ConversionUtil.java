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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.basho.riak.client.http.util.ClientUtils;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.query.LinkWalkStep.Accumulate;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedFunction;
import com.basho.riak.client.query.functions.NamedJSFunction;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.IntIndex;
import com.basho.riak.client.raw.DeleteMeta;
import com.basho.riak.client.raw.FetchMeta;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.util.CharsetUtils;
import com.basho.riak.client.util.UnmodifiableIterator;
import com.basho.riak.pbc.CommitHook;
import com.basho.riak.pbc.FetchResponse;
import com.basho.riak.pbc.MapReduceResponseSource;
import com.basho.riak.pbc.ModuleFunction;
import com.basho.riak.pbc.RequestMeta;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

/**
 * 
 * @author russell
 * 
 */
public final class ConversionUtil {

    /**
     * the string to append to the content-type, before the charset name
     */
    private static final String CHARSET = "; charset=";

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
     * Deal with a more detailed response (maybe from a conditional fetch)
     * 
     * @param fetchResponse
     * @return a {@link RiakResponse}
     */
    static RiakResponse convert(FetchResponse fetchResponse) {
        if (fetchResponse.isUnchanged()) {
            return RiakResponse.unmodified();
        }
        
        return convert(fetchResponse.getObjects());
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
        builder.withDeleted(o.getDeleted());

        Date lastModified = o.getLastModified();

        if (lastModified != null) {
            builder.withLastModified(lastModified.getTime());
        }

        final Collection<RiakLink> links = new ArrayList<RiakLink>();

        for (com.basho.riak.pbc.RiakLink link : o.getLinks()) {
            links.add(convert(link));
        }

        builder.withLinks(links);

        @SuppressWarnings("rawtypes") final Collection<com.basho.riak.client.http.RiakIndex> indexes = o.getIndexes();

        for (@SuppressWarnings("rawtypes") com.basho.riak.client.http.RiakIndex i : indexes) {
            if (i instanceof com.basho.riak.client.http.IntIndex) {
                builder.addIndex(i.getName(), (Long) i.getValue());
            }
            if (i instanceof com.basho.riak.client.http.BinIndex) {
                builder.addIndex(i.getName(), (String) i.getValue());
            }
        }

        String ctype = o.getContentType();
        String charset = o.getCharset();

        if(CharsetUtils.hasCharset(ctype) || charset==null || "".equals(charset.trim())) {
            builder.withContentType(ctype);
        } else {
            builder.withContentType(ctype + CHARSET + charset);
        }

        final Map<String, String> userMetaData = new HashMap<String, String>(o.getUsermeta());

        builder.withUsermeta(userMetaData);

        return builder.build();
    }

    /**
     * @param link
     * @return
     */
    private static RiakLink convert(com.basho.riak.pbc.RiakLink link) {
        return new RiakLink(nullSafeToStringUtf8(link.getBucket()), nullSafeToStringUtf8(link.getKey()),
                            nullSafeToStringUtf8(link.getTag()));
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
     * @param vclock
     * @return the vclock bytes, or null if vclock is null
     */
    static byte[] nullSafeToBytes(VClock vclock) {
        return vclock == null ? null : vclock.getBytes();
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
            requestMeta.w(storeMeta.getW().getIntValue());
        }

        if (storeMeta.hasDw()) {
            requestMeta.dw(storeMeta.getDw().getIntValue());
        }

        if (storeMeta.hasReturnBody()) {
            requestMeta.returnBody(storeMeta.getReturnBody());
        }

        if (storeMeta.hasReturnHead()) {
            requestMeta.returnHead(storeMeta.getReturnHead());
        }

        String contentType = riakObject.getContentType();
        if (contentType != null) {
            requestMeta.contentType(contentType);
        }

        if (storeMeta.hasPw()) {
            requestMeta.pw( storeMeta.getPw().getIntValue());
        }

        if (storeMeta.hasIfNoneMatch()) {
            requestMeta.ifNoneMatch(storeMeta.getIfNoneMatch());
        }

        if (storeMeta.hasIfNotModified()) {
            requestMeta.ifNotModified(storeMeta.getIfNotModified());
        }

        if (storeMeta.hasAsis()) {
            requestMeta.asis(storeMeta.getAsis());
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

        // copy the indexes
        for (Map.Entry<IntIndex, Set<Long>> i : riakObject.allIntIndexesV2().entrySet()) {
            String name = i.getKey().getFullname();
            for (Long v : i.getValue()) {
                result.addIndex(name, v);
            }
        }

        // copy the indexes
        for (Map.Entry<BinIndex, Set<String>> i : riakObject.allBinIndexes().entrySet()) {
            String name = i.getKey().getFullname();
            for (String v : i.getValue()) {
                result.addIndex(name, v);
            }
        }

        String ctype = riakObject.getContentType();
        if(CharsetUtils.hasCharset(ctype)) {
            result.setCharset(CharsetUtils.getDeclaredCharset(ctype));
            ctype = ctype.split(";")[0];
        }

        result.setContentType(ctype);
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
        com.basho.riak.pbc.BucketProperties props = 
            new com.basho.riak.pbc.BucketProperties()
            .nValue(p.getNVal())
            .allowMult(p.getAllowSiblings())
            .lastWriteWins(p.getLastWriteWins())
            .backend(p.getBackend())
            .smallVClock(p.getSmallVClock())
            .bigVClock(p.getBigVClock())
            .youngVClock(p.getYoungVClock())
            .oldVclock(p.getOldVClock())
            .r(p.getR() == null ? null : p.getR().getIntValue())
            .w(p.getW() == null ? null : p.getW().getIntValue())
            .rw(p.getRW() == null ? null : p.getRW().getIntValue())
            .dw(p.getDW() == null ? null : p.getDW().getIntValue())
            .pr(p.getPR() == null ? null : p.getPR().getIntValue())
            .pw(p.getPW() == null ? null : p.getPW().getIntValue())
            .basicQuorum(p.getBasicQuorum())
            .notFoundOk(p.getNotFoundOK())
            .searchEnabled(p.getSearch());
        
        if (p.getPrecommitHooks() != null) {
            List<CommitHook> hooklist = new ArrayList<CommitHook>();
            for (NamedFunction f : p.getPrecommitHooks()) {
                if (f instanceof NamedJSFunction) {
                    hooklist.add(new CommitHook(((NamedJSFunction) f).getFunction()));
                } else {
                    hooklist.add(new CommitHook(((NamedErlangFunction)f).getMod(),
                                                ((NamedErlangFunction)f).getFun()));
                }
            }
            props.precommitHooks(hooklist);
        }
            
        if (p.getPostcommitHooks() != null) {
            List<CommitHook> hooklist = new ArrayList<CommitHook>();
            for (NamedErlangFunction f : p.getPostcommitHooks()) {
                hooklist.add(new CommitHook(f.getMod(), f.getFun()));
            }
            props.postcommitHooks(hooklist);
        }
        
        if (p.getLinkWalkFunction() != null) {
            props.linkFun(new ModuleFunction(p.getLinkWalkFunction().getMod(), p.getLinkWalkFunction().getFun()));
        }
            
        if (p.getChashKeyFunction() != null) {
            props.cHashFun(new ModuleFunction(p.getChashKeyFunction().getMod(), p.getChashKeyFunction().getFun()));
        }
        
        return props;
    }

    /**
     * @param properties
     * @return
     */
    static BucketProperties convert(com.basho.riak.pbc.BucketProperties properties) {
        BucketPropertiesBuilder builder =  new BucketPropertiesBuilder()
            .allowSiblings(properties.getAllowMult())
            .nVal(properties.getNValue())   
            .lastWriteWins(properties.getLastWriteWins())
            .backend(properties.getBackend())
            .smallVClock(properties.getSmallVClock())
            .bigVClock(properties.getBigVClock())
            .youngVClock(properties.getYoungVClock())
            .oldVClock(properties.getOldVClock())
            .r(properties.getR())
            .w(properties.getW())
            .rw(properties.getRw())
            .dw(properties.getDw())
            .pr(properties.getPr())
            .pw(properties.getPw())
            .basicQuorum(properties.getBasicQuorum())
            .notFoundOK(properties.getNotFoundOk())
            .search(properties.getSearchEnabled());
        
            for (CommitHook hook : properties.getPrecommitHooks()) {
                if (hook.isJavascript()) {
                    builder.addPrecommitHook(new NamedJSFunction(hook.getJsName()));
                } else {
                    builder.addPrecommitHook(new NamedErlangFunction(hook.getErlModule(), 
                                                                     hook.getErlFunction()));
                }
            }
        
            for (CommitHook hook : properties.getPostcommitHooks()) {
                if (hook.isJavascript()) {
                    throw new IllegalArgumentException("Post-commit hooks can only be erlang");
                } else {
                    builder.addPostcommitHook(new NamedErlangFunction(hook.getErlModule(), 
                                                                     hook.getErlFunction()));
                }
            }
            
            if (properties.getcHashFun() != null) {
                builder.chashKeyFunction(new NamedErlangFunction(properties.getcHashFun().getModule(),
                                                                 properties.getcHashFun().getFunction()));
            }
            
            if (properties.getLinkFun() != null) {
                builder.linkWalkFunction(new NamedErlangFunction(properties.getLinkFun().getModule(),
                                                                 properties.getLinkFun().getFunction()));
            }
            
            return builder.build();
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

            Date lastModDate = ClientUtils.parseDate((String) meta.get("X-Riak-Last-Modified"));
            if (lastModDate != null)
                b.withLastModified(lastModDate.getTime());

            List<List<String>> links = (List<List<String>>) meta.get("Links");
            if (links != null) {
                for (List<String> link : links) {
                    b.addLink(link.get(0), link.get(1), link.get(2));
                }
            }

            Map<String, String> userMetaData = (Map<String, String>) meta.get("X-Riak-Meta");
            b.withUsermeta(userMetaData);
        }
        return b.build();
    }

    /**
     * Convert a {@link FetchMeta} to a {@link com.basho.riak.pbc.FetchMeta}
     * @param fm the {@link FetchMeta} to convert
     * @return the {@link com.basho.riak.pbc.FetchMeta} with the same values
     */
    static com.basho.riak.pbc.FetchMeta convert(FetchMeta fm) {
        if (fm != null) {
            return new com.basho.riak.pbc.FetchMeta( fm.hasR() ? fm.getR().getIntValue() : null, 
                                                     fm.hasPr() ? fm.getPr().getIntValue() : null, 
                                                     fm.getNotFoundOK(), 
                                                     fm.getBasicQuorum(),
                                                     fm.getHeadOnly(), 
                                                     fm.getReturnDeletedVClock(), 
                                                     fm.getIfModifiedVClock()
                                                   );
        } else {
            return com.basho.riak.pbc.FetchMeta.empty();
        }
    }

    /**
     * Convert a {@link DeleteMeta} to a {@link com.basho.riak.pbc.DeleteMeta}
     * @param dm the {@link FetchMeta} to convert
     * @return the {@link com.basho.riak.pbc.FetchMeta} with the same values
     */
    static com.basho.riak.pbc.DeleteMeta convert(DeleteMeta dm) {
        if (dm != null) {
            return new com.basho.riak.pbc.DeleteMeta( dm.hasR() ? dm.getR().getIntValue() : null, 
                                                      dm.hasPr() ? dm.getPr().getIntValue() : null, 
                                                      dm.hasW() ? dm.getW().getIntValue() : null, 
                                                      dm.hasDw() ? dm.getDw().getIntValue() : null, 
                                                      dm.hasPw() ? dm.getPw().getIntValue() : null , 
                                                      dm.hasRw() ? dm.getRw().getIntValue() : null, 
                                                      nullSafeToBytes(dm.getVclock())
                                                    );
        } else {
            return com.basho.riak.pbc.DeleteMeta.empty();
        }
    }
}