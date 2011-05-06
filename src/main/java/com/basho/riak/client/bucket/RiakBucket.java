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

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.builders.DomainBucketBuilder;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.Converter;

/**
 * A DomainBucket<RiakObject> for convenience.
 * @author russell
 * 
 */
public class RiakBucket {

    private final DomainBucket<IRiakObject> delegate;
    private final Bucket bucket;

    public static RiakBucket newRiakBucket(final Bucket b) {
        // create a DomainBucket as a delegate
        DomainBucketBuilder<IRiakObject> builder = DomainBucket.builder(b, IRiakObject.class);
        builder.withConverter(new Converter<IRiakObject>() {
            // no conversion required
            public IRiakObject toDomain(IRiakObject riakObject) throws ConversionException {
                return riakObject;
            }

            public IRiakObject fromDomain(IRiakObject domainObject, VClock vclock) throws ConversionException {
                return domainObject;
            }
        });

        return new RiakBucket(builder.build(), b);
    }

    private RiakBucket(final DomainBucket<IRiakObject> delegate, final Bucket bucket) {
        this.delegate = delegate;
        this.bucket = bucket;
    }

    /**
     * @param o
     * @return
     * @throws RiakException
     * @see com.basho.riak.client.bucket.DomainBucket#store(java.lang.Object)
     */
    public IRiakObject store(IRiakObject o) throws RiakException {
        return delegate.store(o);
    }

    /**
     * Convenience for storing a String in Riak.
     * @param key
     * @param value
     * @return
     * @throws RiakException
     */
    public IRiakObject store(String key, byte[] value) throws RiakException {
        return delegate.store(RiakObjectBuilder.newBuilder(bucket.getName(), key).withValue(value).build());
    }
    /**
     * @param key
     * @return
     * @throws RiakException
     * @see com.basho.riak.client.bucket.DomainBucket#fetch(java.lang.String)
     */
    public IRiakObject fetch(String key) throws RiakException {
        return delegate.fetch(key);
    }

    /**
     * @param o
     * @return
     * @throws RiakException
     * @see com.basho.riak.client.bucket.DomainBucket#fetch(java.lang.Object)
     */
    public IRiakObject fetch(IRiakObject o) throws RiakException {
        return delegate.fetch(o);
    }

    /**
     * @param o
     * @throws RiakException
     * @see com.basho.riak.client.bucket.DomainBucket#delete(java.lang.Object)
     */
    public void delete(IRiakObject o) throws RiakException {
        delegate.delete(o);
    }

    /**
     * @param key
     * @throws RiakException
     * @see com.basho.riak.client.bucket.DomainBucket#delete(java.lang.String)
     */
    public void delete(String key) throws RiakException {
        delegate.delete(key);
    }

}
