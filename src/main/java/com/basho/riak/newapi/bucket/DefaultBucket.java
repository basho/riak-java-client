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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.newapi.RiakException;
import com.basho.riak.newapi.RiakObject;
import com.basho.riak.newapi.builders.RiakObjectBuilder;
import com.basho.riak.newapi.cap.ConflictResolver;
import com.basho.riak.newapi.cap.Mutation;
import com.basho.riak.newapi.cap.Quorum;
import com.basho.riak.newapi.cap.UnresolvedConflictException;
import com.basho.riak.newapi.convert.Converter;
import com.basho.riak.newapi.operations.DeleteObject;
import com.basho.riak.newapi.operations.FetchObject;
import com.basho.riak.newapi.operations.StoreObject;
import com.basho.riak.newapi.query.NamedErlangFunction;
import com.basho.riak.newapi.query.NamedFunction;

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

    /**
     * Iterate over the keys for this bucket (Expensive, are you sure?)
     */
    public Iterator<String> keys() throws RiakException {
        try {
            return client.fetchBucketKeys(name);
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
                return original.setValue(value);
            }
        }).withResolver(new ConflictResolver<RiakObject>() {

            public RiakObject resolve(Collection<RiakObject> siblings) throws UnresolvedConflictException {
                if (siblings.size() > 1) {
                    throw new UnresolvedConflictException("Siblings found", siblings);
                } else if (siblings.size() == 1) {
                    return siblings.iterator().next();
                } else {
                    return RiakObjectBuilder.newBuilder(b, key).build();
                }
            }
        }).withConverter(new Converter<RiakObject>() {

            public RiakObject toDomain(RiakObject riakObject) {
                return riakObject;
            }

            public RiakObject fromDomain(RiakObject domainObject) {
                return domainObject;
            }
        });
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#store(java.lang.Object)
     */
    public <T> StoreObject<T> store(T o) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#fetch(java.lang.String,
     * java.lang.Class)
     */
    public <T> FetchObject<T> fetch(String key, Class<T> type) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#fetch(java.lang.Object)
     */
    public <T> FetchObject<T> fetch(T o) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.bucket.Bucket#delete(java.lang.Object)
     */
    public <T> DeleteObject<T> delete(T o) {
        return null;
    }
}
