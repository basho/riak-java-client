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

import com.basho.riak.newapi.RiakException;
import com.basho.riak.newapi.RiakObject;
import com.basho.riak.newapi.builders.DomainBucketBuilder;
import com.basho.riak.newapi.builders.RiakObjectBuilder;
import com.basho.riak.newapi.cap.VClock;
import com.basho.riak.newapi.convert.ConversionException;
import com.basho.riak.newapi.convert.Converter;

/**
 * A DomainBucket<RiakObject> for convenience.
 * @author russell
 * 
 */
public class RiakBucket {

    private final DomainBucket<RiakObject> delegate;
    private final Bucket bucket;

    public static RiakBucket newRiakBucket(final Bucket b) {
        // create a DomainBucket as a delegate
        DomainBucketBuilder<RiakObject> builder = DomainBucket.builder(b, RiakObject.class);
        builder.withConverter(new Converter<RiakObject>() {
            // no conversion required
            public RiakObject toDomain(RiakObject riakObject) throws ConversionException {
                return riakObject;
            }

            public RiakObject fromDomain(RiakObject domainObject, VClock vclock) throws ConversionException {
                return domainObject;
            }
        });

        return new RiakBucket(builder.build(), b);
    }

    private RiakBucket(final DomainBucket<RiakObject> delegate, final Bucket bucket) {
        this.delegate = delegate;
        this.bucket = bucket;
    }

    /**
     * @param o
     * @return
     * @throws RiakException
     * @see com.basho.riak.newapi.bucket.DomainBucket#store(java.lang.Object)
     */
    public RiakObject store(RiakObject o) throws RiakException {
        return delegate.store(o);
    }

    /**
     * Convenience for storing a String in Riak.
     * @param key
     * @param value
     * @return
     * @throws RiakException
     */
    public RiakObject store(String key, String value) throws RiakException {
        return delegate.store(RiakObjectBuilder.newBuilder(bucket.getName(), key).withValue(value).build());
    }
    /**
     * @param key
     * @return
     * @throws RiakException
     * @see com.basho.riak.newapi.bucket.DomainBucket#fetch(java.lang.String)
     */
    public RiakObject fetch(String key) throws RiakException {
        return delegate.fetch(key);
    }

    /**
     * @param o
     * @return
     * @throws RiakException
     * @see com.basho.riak.newapi.bucket.DomainBucket#fetch(java.lang.Object)
     */
    public RiakObject fetch(RiakObject o) throws RiakException {
        return delegate.fetch(o);
    }

    /**
     * @param o
     * @throws RiakException
     * @see com.basho.riak.newapi.bucket.DomainBucket#delete(java.lang.Object)
     */
    public void delete(RiakObject o) throws RiakException {
        delegate.delete(o);
    }

    /**
     * @param key
     * @throws RiakException
     * @see com.basho.riak.newapi.bucket.DomainBucket#delete(java.lang.String)
     */
    public void delete(String key) throws RiakException {
        delegate.delete(key);
    }

}
