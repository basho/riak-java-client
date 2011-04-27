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
package com.basho.riak.newapi.bucket;

import static com.basho.riak.newapi.convert.ConversionUtil.getKey;

import java.io.IOException;
import java.util.Collection;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.newapi.RiakException;
import com.basho.riak.newapi.RiakObject;
import com.basho.riak.newapi.builders.RiakObjectBuilder;
import com.basho.riak.newapi.cap.DefaultResolver;
import com.basho.riak.newapi.cap.Mutation;
import com.basho.riak.newapi.cap.Quorum;
import com.basho.riak.newapi.cap.VClock;
import com.basho.riak.newapi.convert.ConversionException;
import com.basho.riak.newapi.convert.Converter;
import com.basho.riak.newapi.convert.JSONConverter;
import com.basho.riak.newapi.convert.NoKeySpecifedException;
import com.basho.riak.newapi.operations.DeleteObject;
import com.basho.riak.newapi.operations.FetchObject;
import com.basho.riak.newapi.operations.StoreObject;
import com.basho.riak.newapi.query.functions.NamedErlangFunction;
import com.basho.riak.newapi.query.functions.NamedFunction;

/**
 * @author russell
 * 
 */
public class DefaultBucket implements Bucket {

    private final String name;
    private final BucketProperties properties;
    private final RawClient client;

    /**
     * @param properties
     * @param client
     */
    protected DefaultBucket(String name, BucketProperties properties, RawClient client) {
        this.name = name;
        this.properties = properties;
        this.client = client;
    }

    // / BUCKET PROPS

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
    public int getSmallVClock() {
        return properties.getSmallVClock();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getBigVClock()
     */
    public int getBigVClock() {
        return properties.getBigVClock();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getYoungVClock()
     */
    public long getYoungVClock() {
        return properties.getYoungVClock();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.BucketProperties#getOldVClock()
     */
    public long getOldVClock() {
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

    // / BUCKET

    /**
     * Iterate over the keys for this bucket (Expensive, are you sure?)
     */
    public Iterable<String> keys() throws RiakException {
        try {
            return client.listKeys(name);
        } catch (IOException e) {
            throw new RiakException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.Bucket#store(java.lang.String,
     * java.lang.String)
     */
    public StoreObject<RiakObject> store(final String key, final String value) {
        final Bucket b = this;

        return new StoreObject<RiakObject>(client, b, key).withMutator(new Mutation<RiakObject>() {
            public RiakObject apply(RiakObject original) {
                if (original == null) {
                    return RiakObjectBuilder.newBuilder(b, key).withValue(value).build();
                } else {
                    return original.setValue(value);
                }
            }
        }).withResolver(new DefaultResolver<RiakObject>()).withConverter(new Converter<RiakObject>() {

            public RiakObject toDomain(RiakObject riakObject) {
                return riakObject;
            }

            public RiakObject fromDomain(RiakObject domainObject, VClock vclock) throws ConversionException {
                return domainObject;
            }
        });
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#store(java.lang.Object)
     */
    public <T> StoreObject<T> store(final T o) {
        final Bucket b = this;
        @SuppressWarnings("unchecked") Class<T> clazz = (Class<T>) o.getClass();
        final String key = getKey(o);
        if (key == null) {
            throw new NoKeySpecifedException(o);
        }
        return new StoreObject<T>(client, b, key).withConverter(new JSONConverter<T>(clazz, b)).withMutator(new Mutation<T>() {
                                                                                                                public T apply(T original) {
                                                                                                                    return o;
                                                                                                                };
                                                                                                            }).withResolver(new DefaultResolver<T>());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#store(java.lang.String,
     * java.lang.Object)
     */
    public <T> StoreObject<T> store(final String key, final T o) {
        final Bucket b = this;
        @SuppressWarnings("unchecked") final Class<T> clazz = (Class<T>) o.getClass();

        return new StoreObject<T>(client, b, key).withConverter(new JSONConverter<T>(clazz, b, key)).withMutator(new Mutation<T>() {
                                                                                                                     public T apply(T original) {
                                                                                                                         return o;
                                                                                                                     };
                                                                                                                 }).withResolver(new DefaultResolver<T>());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#fetch(java.lang.Object)
     */
    public <T> FetchObject<T> fetch(T o) {
        final Bucket b = this;
        @SuppressWarnings("unchecked") final Class<T> clazz = (Class<T>) o.getClass();
        final String key = getKey(o);
        if (key == null) {
            throw new NoKeySpecifedException(o);
        }
        return new FetchObject<T>(client, this, key).withConverter(new JSONConverter<T>(clazz, b)).withResolver(new DefaultResolver<T>());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#fetch(java.lang.String,
     * java.lang.Class)
     */
    public <T> FetchObject<T> fetch(final String key, final Class<T> type) {
        final Bucket b = this;
        return new FetchObject<T>(client, this, key).withConverter(new JSONConverter<T>(type, b)).withResolver(new DefaultResolver<T>());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#fetch(java.lang.String)
     */
    public FetchObject<RiakObject> fetch(String key) {
        final Bucket b = this;

        return new FetchObject<RiakObject>(client, b, key).withResolver(new DefaultResolver<RiakObject>()).withConverter(new Converter<RiakObject>() {

                                                                                                                             public RiakObject toDomain(RiakObject riakObject) {
                                                                                                                                 return riakObject;
                                                                                                                             }

                                                                                                                             public RiakObject fromDomain(RiakObject domainObject,
                                                                                                                                                          VClock vclock)
                                                                                                                                     throws ConversionException {
                                                                                                                                 return RiakObjectBuilder.from(domainObject).withVClock(vclock).build();
                                                                                                                             }
                                                                                                                         });
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
        return new DeleteObject(client, this, key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#delete(java.lang.String)
     */
    public DeleteObject delete(String key) {
        return new DeleteObject(client, this, key);
    }

}
