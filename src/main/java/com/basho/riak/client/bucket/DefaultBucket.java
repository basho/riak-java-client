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
package com.basho.riak.client.bucket;

import static com.basho.riak.client.convert.KeyUtil.getKey;

import java.io.IOException;
import java.util.Collection;

import com.basho.riak.client.DefaultRiakClient;
import com.basho.riak.client.DefaultRiakObject;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.cap.ClobberMutation;
import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.cap.Mutation;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.JSONConverter;
import com.basho.riak.client.convert.NoKeySpecifedException;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.operations.CounterObject;
import com.basho.riak.client.operations.DeleteObject;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.operations.MultiFetchObject;
import com.basho.riak.client.operations.RiakOperation;
import com.basho.riak.client.operations.StoreObject;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedFunction;
import com.basho.riak.client.query.indexes.FetchIndex;
import com.basho.riak.client.query.indexes.RiakIndex;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.query.StreamingOperation;
import com.basho.riak.client.util.CharsetUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Default implementation of {@link Bucket} for creating {@link RiakOperation}s
 * on k/v data and accessing {@link BucketProperties}.
 * 
 * <p>
 * Obtain a {@link DefaultBucket} from {@link FetchBucket} or
 * {@link WriteBucket} operations from
 * {@link DefaultRiakClient#fetchBucket(String)},
 * {@link DefaultRiakClient#createBucket(String)}
 * </p>
 * <p>
 * <code><pre>
 *   final String bucketName = UUID.randomUUID().toString();
 *   
 *   Bucket b = client.createBucket(bucketName).execute();
 *   //store something
 *   IRiakObject o = b.store("k", "v").execute();
 *   //fetch it back
 *   IRiakObject fetched = b.fetch("k").execute();
 *   // now update that riak object
 *   b.store("k", "my new value").execute();
 *   //fetch it back again
 *   fetched = b.fetch("k").execute();
 *   //delete it
 *   b.delete("k").execute();
 * </pre></code>
 * <p>
 * All operations created by instances of this class are configured with the
 * {@link Retrier} and {@link RawClient} passed at construction.
 * </p>
 * 
 * @author russell
 * @see DomainBucket
 * @see RiakBucket
 */
public class DefaultBucket implements Bucket {

    private final String name;
    private final BucketProperties properties;
    private final RawClient client;
    private final Retrier retrier;

    /**
     * All {@link RiakOperation}s created by this instance will use the
     * {@link RawClient} and {@link Retrier} provided here.
     * 
     * @param name this bucket's name
     * @param properties the {@link BucketProperties} for this bucket
     * @param client a {@link RawClient} to use for all {@link RiakOperation}s
     * @param retrier a {@link Retrier} to use for all {@link RiakOperation}s
     */
    protected DefaultBucket(String name, final BucketProperties properties, final RawClient client, final Retrier retrier) {
        this.name = name;
        this.properties = properties;
        this.client = client;
        this.retrier = retrier;
    }

    // BUCKET PROPS

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#getName()
     */
    public String getName() {
        return name;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getAllowSiblings()
     */
    public Boolean getAllowSiblings() {
        return properties.getAllowSiblings();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getLastWriteWins()
     */
    public Boolean getLastWriteWins() {
        return properties.getLastWriteWins();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getNVal()
     */
    public Integer getNVal() {
        return properties.getNVal();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getBackend()
     */
    public String getBackend() {
        return properties.getBackend();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getSmallVClock()
     */
    public Integer getSmallVClock() {
        return properties.getSmallVClock();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getBigVClock()
     */
    public Integer getBigVClock() {
        return properties.getBigVClock();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getYoungVClock()
     */
    public Long getYoungVClock() {
        return properties.getYoungVClock();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getOldVClock()
     */
    public Long getOldVClock() {
        return properties.getOldVClock();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getPrecommitHooks()
     */
    public Collection<NamedFunction> getPrecommitHooks() {
        return properties.getPrecommitHooks();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getPostcommitHooks()
     */
    public Collection<NamedErlangFunction> getPostcommitHooks() {
        return properties.getPostcommitHooks();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getR()
     */
    public Quorum getR() {
        return properties.getR();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getW()
     */
    public Quorum getW() {
        return properties.getW();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getRW()
     */
    public Quorum getRW() {
        return properties.getRW();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getDW()
     */
    public Quorum getDW() {
        return properties.getDW();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getPR()
     */
    public Quorum getPR() {
        return properties.getPR();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getPW()
     */
    public Quorum getPW() {
        return properties.getPW();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#isBasicQuorum()
     */
    public Boolean getBasicQuorum() {
        return properties.getBasicQuorum();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#isNotFoundOK()
     */
    public Boolean getNotFoundOK() {
        return properties.getNotFoundOK();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getChashKeyFunction()
     */
    public NamedErlangFunction getChashKeyFunction() {
        return properties.getChashKeyFunction();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getLinkWalkFunction()
     */
    public NamedErlangFunction getLinkWalkFunction() {
        return properties.getLinkWalkFunction();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#getSearch()
     */
    public Boolean getSearch() {
        return properties.getSearch();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.BucketProperties#isSearchEnabled()
     */
    public boolean isSearchEnabled() {
        return properties.isSearchEnabled();
    }

    // BUCKET

    /**
     * Iterate over the keys for this bucket (Expensive, are you sure?) Beware:
     * at present all {@link RawClient#listKeys(String)} operations return a
     * stream of keys. 
     * 
     * You *must* call {@link StreamingOperation#cancel() } on the returned
     * {@link StreamingOperation} if you do not iterate through the entire set.
     * 
     * As a safeguard the stream is closed automatically when the iterator is
     * weakly reachable but due to the nature of the GC it is inadvisable to 
     * rely on this to close the iterator. Do not retain a reference to this {@link Iterable}
     * after you have used it.
     * 
     * @see RawClient#listKeys(String)
     */
    public StreamingOperation<String> keys() throws RiakException {
        try {
            return client.listKeys(name);
        } catch (IOException e) {
            throw new RiakException(e);
        }
    }

    /**
     * Convenience method to create a RiakObject with a payload of
     * application/octect-stream
     * <p>
     * For example, to get a new {@link IRiakObject} into Riak. <code><pre>
     * IRiakObject myNewObject = bucket.store("k", myByteArray)
     *                              .w(2)  // tunable CAP write quorum
     *                              .returnBody(true) // return the IRiakObject from the store
     *                              .execute(); // perform the operation.
     * </pre></code>
     * </p>
     * <p>
     * Creates a {@link StoreObject} operation configured with a
     * {@link Mutation} that copies <code>value</code> and
     * {@link DefaultRiakObject#DEFAULT_CONTENT_TYPE} over the any existing
     * value at <code>key</code> or creates a new {@link DefaultRiakObject} with
     * <code>value</code> and {@link DefaultRiakObject#DEFAULT_CONTENT_TYPE}.
     * </p>
     * <p>
     * The {@link StoreObject} is configured with the {@link DefaultResolver}
     * which means the presence of siblings triggers a
     * {@link UnresolvedConflictException}
     * </p>
     * <p>
     * The {@link StoreObject} is configured with a {@link Converter} that
     * simply returns what it is given (IE does no conversion).
     * </p>
     * 
     * @param key
     *            the key to store the object under.
     * @param value
     *            a byte[] of the objects value.
     * @return a {@link StoreObject} configured to store <code>value</code> at
     *         <code>key</code> on <code>execute()</code>.
     * @see StoreObject
     */
    public StoreObject<IRiakObject> store(final String key, final byte[] value) {

        return new StoreObject<IRiakObject>(client, name, key, retrier).withMutator(new Mutation<IRiakObject>() {
            public IRiakObject apply(IRiakObject original) {
                if (original == null) {
                    return RiakObjectBuilder.newBuilder(name, key).withValue(value).withContentType(Constants.CTYPE_OCTET_STREAM).build();
                } else {
                    original.setValue(value);
                    return original;
                }
            }
        }).withResolver(new DefaultResolver<IRiakObject>()).withConverter(new PassThroughConverter());
    }

    /**
     * Convenience methods will create an {@link IRiakObject} with
     * <code>value</code> as the data payload and
     * <code>text/plain:charset=utf-8</code> as the <code>contentType</code>
     * <p>
     * For example, to get a new {@link IRiakObject} into Riak. <code><pre>
     * IRiakObject myNewObject = bucket.store("k", "myValue")
     *                              .w(2)  // tunable CAP write quorum
     *                              .returnBody(true) // return the IRiakObject from the store
     *                              .execute(); // perform the operation.
     * </pre></code>
     * </p>
      * <p>
     * Creates a {@link StoreObject} operation configured with a
     * {@link Mutation} that copies <code>value</code> and
     * {@link Constants#CTYPE_TEXT_UTF8} over the any existing
     * value at <code>key</code> or creates a new {@link DefaultRiakObject} with
     * <code>value</code> and {@link Constants#CTYPE_TEXT_UTF8}.
     * </p>
     * <p>
     * The {@link StoreObject} is configured with the {@link DefaultResolver}
     * which means the presence of siblings triggers a
     * {@link UnresolvedConflictException}
     * </p>
     * <p>
     * The {@link StoreObject} is configured with a {@link Converter} that
     * simply returns what it is given (IE does no conversion).
     * </p>
     * 
     * @param key
     *            the key to store the object under.
     * @param value
     *            a String of the data to store
     * @return a {@link StoreObject} configured to store <code>value</code> at
     *         <code>key</code> on <code>execute()</code>.
     * @see StoreObject
     */
    public StoreObject<IRiakObject> store(final String key, final String value) {
        final Mutation<IRiakObject> m = new Mutation<IRiakObject>() {
            public IRiakObject apply(IRiakObject original) {
                if (original == null) {
                    return RiakObjectBuilder.newBuilder(name, key).withValue(value).withContentType(Constants.CTYPE_TEXT_UTF8).build();
                } else {
                    original.setValue(CharsetUtils.utf8StringToBytes(value));
                    original.setContentType(Constants.CTYPE_TEXT_UTF8);
                    return original;
                }
            }
        };

        return store(key, CharsetUtils.utf8StringToBytes(value)).withMutator(m);
    }

    /**
     * Store an instance of <code>T</code> in Riak. Depends on the
     * {@link Converter} provided to {@link StoreObject} to convert
     * <code>o</code> from <code>T</code> to {@link IRiakObject}.
     * <p>
     * <code>T</code> must have a field annotated with {@link RiakKey} as the
     * Key to store this data under.
     * </p>
     * 
     * <p>
     * Creates a {@link StoreObject} operation configured with the
     * {@link JSONConverter} the {@link ClobberMutation} and
     * {@link DefaultResolver}.
     * </p>
     * 
     * @param <T>
     *            the Type of <code>o</code>
     * @param o
     *            the data to store
     * @return a {@link StoreObject} configured to store <code>o</code> at the
     *         {@link RiakKey} annotated <code>key</code> on
     *         <code>execute()</code>.
     * @see StoreObject
     * @see DomainBucket
     */
    public <T> StoreObject<T> store(final T o) {
        @SuppressWarnings("unchecked") Class<T> clazz = (Class<T>) o.getClass();
        final String key = getKey(o);
        
        Converter<T> converter = getDefaultConverter(clazz);

        return new StoreObject<T>(client, name, key, retrier)
            .withConverter(converter)
                .withMutator(new ClobberMutation<T>(o))
                  .withResolver(new DefaultResolver<T>());
    }

    private <T> Converter<T> getDefaultConverter(Class<T> clazz) {
        return getDefaultConverter(clazz, null);
    }

    @SuppressWarnings("unchecked") private <T> Converter<T> getDefaultConverter(Class<T> clazz, String key) {
        Converter<T> converter;
        if (IRiakObject.class.isAssignableFrom(clazz)) {
            converter = (Converter<T>) new PassThroughConverter();
        } else {
            if (key != null) {
                converter = new JSONConverter<T>(clazz, name, key);
            } else {
                converter = new JSONConverter<T>(clazz, name);
            }
        }
        return converter;
    }

    /**
     * Store an instance of <code>T</code> in Riak. Depends on the
     * {@link Converter} provided to {@link StoreObject} to convert
     * <code>o</code> from <code>T</code> to {@link IRiakObject}.
     * 
     * <p>
     * Creates a {@link StoreObject} operation configured with the
     * {@link JSONConverter} the {@link ClobberMutation} and
     * {@link DefaultResolver}.
     * </p>
     * 
     * @param <T>
     *            the Type of <code>o</code>
     * @param o
     *            the data to store
     * @return a {@link StoreObject} configured to store <code>o</code> at the
     *         {@link RiakKey} annotated <code>key</code> on
     *         <code>execute()</code>.
     * @see StoreObject
     * @see DomainBucket
     */
    public <T> StoreObject<T> store(final String key, final T o) {
        @SuppressWarnings("unchecked") final Class<T> clazz = (Class<T>) o.getClass();
        Converter<T> converter = getDefaultConverter(clazz, key);
        return new StoreObject<T>(client, name, key, retrier).
        withConverter(converter)
            .withMutator(new ClobberMutation<T>(o)).withResolver(new DefaultResolver<T>());
    }

    /**
     * Creates a {@link FetchObject} operation that returns the data at
     * <code>o</code>'s annotated {@link RiakKey} field as an instance of type
     * <code>T</code> on <code>execute()</code>.
     * <p>
     * Creates a {@link FetchObject} operation configured with the
     * {@link JSONConverter} and
     * {@link DefaultResolver}.
     * </p>
     * 
     * @param <T>
     *            the Type to return
     * @param o
     *            an instance ot <code>T</code> that has the key annotated with
     *            {@link RiakKey}
     * @return a {@link FetchObject}
     * @see FetchObject
     */
    public <T> FetchObject<T> fetch(T o) {
        @SuppressWarnings("unchecked") final Class<T> clazz = (Class<T>) o.getClass();
        final String key = getKey(o);
        if (key == null) {
            throw new NoKeySpecifedException(o);
        }
        Converter<T> converter = getDefaultConverter(clazz);
        return new FetchObject<T>(client, name, key, retrier)
            .withConverter(converter)
            .withResolver(new DefaultResolver<T>());
    }

    /**
     * Creates a {@link FetchObject} operation that returns the data at
     * <code>key</code> as an instance of type <code>T</code> on
     * <code>execute()</code>.
     * 
     * <p>
     * Creates a {@link FetchObject} operation configured with the
     * {@link JSONConverter} and
     * {@link DefaultResolver}.
     * </p>
     * 
     * @param <T>
     *            the Type to return
     * @param key
     *            the key under which the data is stored
     * @param type
     *            the Class of the type to return
     * @return a {@link FetchObject}
     * @see FetchObject
     */
    public <T> FetchObject<T> fetch(final String key, final Class<T> type) {
        Converter<T> converter = getDefaultConverter(type, key);
        return new FetchObject<T>(client, name, key, retrier)
            .withConverter(converter)
            .withResolver(new DefaultResolver<T>());
    }

    /**
     * Creates a {@link FetchObject} that returns the data at <code>key</code>
     * as an {@link IRiakObject} on <code>execute()</code>.
     * 
     * <p>
     * Creates a {@link FetchObject} with the {@link DefaultResolver} and a {@link Converter}
     * that does nothing to the {@link IRiakObject}.
     * </p>
     * 
     * @param key the key
     * @return a {@link FetchObject}
     * @see FetchObject
     */
    public FetchObject<IRiakObject> fetch(String key) {
        return new FetchObject<IRiakObject>(client, name, key, retrier)
        .withResolver(new DefaultResolver<IRiakObject>())
        .withConverter(new PassThroughConverter());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#delete(java.lang.Object)
     */
    public <T> DeleteObject delete(T o) {
        final String key = getKey(o);
        if (key == null) {
            throw new NoKeySpecifedException(o);
        }
        return new DeleteObject(client, name, key, retrier);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#delete(java.lang.String)
     */
    public DeleteObject delete(String key) {
        return new DeleteObject(client, name, key, retrier);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.bucket.Bucket#fetchIndex(com.basho.riak.client.query.indexes.RiakIndex)
     */
    public <T> FetchIndex<T> fetchIndex(RiakIndex<T> index) {
        return new FetchIndex<T>(client, name, index, retrier);
    }

    /**
     * (non-Javadoc)
     * @see com.basho.riak.client.bucket.Bucket#multiFetch(java.lang.String[]) 
     */
    public MultiFetchObject<IRiakObject> multiFetch(String[] keys)
    {
        return new MultiFetchObject<IRiakObject>(client, name, Arrays.asList(keys), retrier)
        .withResolver(new DefaultResolver<IRiakObject>())
        .withConverter(new PassThroughConverter());
    }

    /**
     * (non-Javadoc)
     * @see com.basho.riak.client.bucket.Bucket#multiFetch(java.util.List, java.lang.Class) 
     */
    public <T> MultiFetchObject<T> multiFetch(List<String> keys, Class<T> type)
    {
        Converter<T> converter = getDefaultConverter(type, keys.get(0));
        return new MultiFetchObject<T>(client, name, keys, retrier)
            .withConverter(converter)
            .withResolver(new DefaultResolver<T>());
    }

    /**
     * (non-Javadoc)
     * @see com.basho.riak.client.bucket.Bucket#multiFetch(java.util.List) 
     */
    public <T> MultiFetchObject<T> multiFetch(List<T> objs)
    {
        T o = objs.get(0);
        @SuppressWarnings("unchecked") final Class<T> clazz = (Class<T>) o.getClass();
        List<String> keyList = new ArrayList<String>(objs.size());
        for (T obj : objs)
        {
            String key = getKey(obj);
            if (key == null) {
                throw new NoKeySpecifedException(o);
            }
            keyList.add(key);
        }
        
        Converter<T> converter = getDefaultConverter(clazz);
        return new MultiFetchObject<T>(client, name, keyList, retrier)
            .withConverter(converter)
            .withResolver(new DefaultResolver<T>());
    }
    
    public CounterObject counter(String counter) {
        return new CounterObject(client, name, counter);
    }
    
}
