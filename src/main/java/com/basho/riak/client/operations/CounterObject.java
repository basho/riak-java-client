/*
 * Copyright 2013 Basho Technologies Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.operations;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.raw.FetchMeta;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.StoreMeta;
import java.io.IOException;

/**
 * A new feature in Riak 1.4 is counters. These are implemented via 
 * convergent data types.
 * <p>
 * A counter is uniquely identified by its name (key) in a bucket. The first time
 * you increment a counter, it is created. A counter can be decremented by incrementing it 
 * by a negative value;
 * </p>
 * 
 * <pre>
 * {@code 
 * // Create and increment a counter by 1
 * CounterObject c = bucket.counter("myCounter");
 * Long l = c.increment(1L).returnValue(true).execute();
 * 
 * // fetch an existing counter value
 * l = bucket.counter("myCounter").execute();
 * }
 * </pre>
 * 
 * @see <a href="http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf">A Comprehensive study of Convergent and Commutative Replicated Data Types</a>
 * @author Brian Roach <roach at basho dot com>
 */
public class CounterObject implements RiakOperation<Long>
{
    private final RawClient client;
    private final String bucket;
    private final String counter;
    private Long increment;
    private final StoreMeta.Builder storeMetaBuilder = new StoreMeta.Builder();
    private final FetchMeta.Builder fetchMetaBuilder = new FetchMeta.Builder();
    
    public CounterObject(RawClient rawClient, String bucket, String counter) {
        this.client = rawClient;
        this.bucket = bucket;
        this.counter = counter;
    }
    
    public CounterObject increment(Long increment) {
        this.increment = increment;
        return this;
    }
    
/**
     * Set the primary write quorum for an increment operation, takes precedence
     * over w.
     * 
     * @param pw
     * @return this
     * @see com.basho.riak.client.raw.StoreMeta.Builder#pw(int) 
     */
    public CounterObject pw(int pw) {
        storeMetaBuilder.pw(pw);
        return this;
    }

    /**
     * Set the primary write quorum for an increment operation, takes precedence
     * over w.
     * 
     * @param pw
     * @return this
     * @see com.basho.riak.client.raw.StoreMeta.Builder#pw(com.basho.riak.client.cap.Quora) 
     */
    public CounterObject pw(Quora pw) {
        storeMetaBuilder.pw(pw);
        return this;
    }
    
    /**
     * Set the primary write quorum for an increment operation, takes precedence
     * over w.
     * 
     * @param pw
     * @return this
     * @see com.basho.riak.client.raw.StoreMeta.Builder#pw(com.basho.riak.client.cap.Quorum) 
     */
    public CounterObject pw(Quorum pw) {
        storeMetaBuilder.pw(pw);
        return this;
    }
    
    /**
     * Set the write quorum for an increment operation
     * @param w
     * @return this
     * @see com.basho.riak.client.raw.StoreMeta.Builder#w(int) 
     */
    public CounterObject w(int w) {
        storeMetaBuilder.w(w);
        return this;
    }

    /**
     * Set the write quorum for an increment operation
     * @param w
     * @return this
     * @see com.basho.riak.client.raw.StoreMeta.Builder#w(com.basho.riak.client.cap.Quora) 
     */
    public CounterObject w(Quora w) {
        storeMetaBuilder.w(w);
        return this;
    }

    /**
     * Set the write quorum for an increment operation
     * @param w
     * @return this
     * @see com.basho.riak.client.raw.StoreMeta.Builder#w(com.basho.riak.client.cap.Quorum) 
     */
    public CounterObject w(Quorum w) {
        storeMetaBuilder.w(w);
        return this;
    }


    /**
     * The durable write quorum for an increment operation
     * @param dw
     * @return this
     * @see com.basho.riak.client.raw.StoreMeta.Builder#dw(int) 
     */
    public CounterObject dw(int dw) {
        storeMetaBuilder.dw(dw);
        return this;
    }

    /**
     * The durable write quorum for for an increment operation
     * @param dw
     * @return this
     * @see com.basho.riak.client.raw.StoreMeta.Builder#dw(com.basho.riak.client.cap.Quora) 
     */
    public CounterObject dw(Quora dw) {
        storeMetaBuilder.dw(dw);
        return this;
    }

    /**
     * The durable write quorum for an increment operation
     * @param dw
     * @return this
     * @see com.basho.riak.client.raw.StoreMeta.Builder#dw(com.basho.riak.client.cap.Quorum) 
     */
    public CounterObject dw(Quorum dw) {
        storeMetaBuilder.dw(dw);
        return this;
    }

    /**
     * Should an increment operation return the new value?
     * @param returnValue
     * @return this
     */
    public CounterObject returnValue(boolean returnValue) {
        storeMetaBuilder.returnBody(returnValue);
        return this;
    }   
    
    /**
     * Set the read quorum for a fetch operation
     *
     * @param r the read quorum for the pre-store fetch
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#r(int) 
     */
    public CounterObject r(int r) {
        this.fetchMetaBuilder.r(r);
        return this;
    }

    /**
     * Set the read quorum for a fetch operation
     * @param r the read quorum for the pre-store fetch
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#r(com.basho.riak.client.cap.Quora) 
     */
    public CounterObject r(Quora r) {
        this.fetchMetaBuilder.r(r);
        return this;
    }
    
    /**
     * Set the read quorum for a fetch operation
     *
     * @param r the read quorum for the pre-store fetch
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#r(com.basho.riak.client.cap.Quorum) 
     */
    public CounterObject r(Quorum r) {
        this.fetchMetaBuilder.r(r);
        return this;
    }
    
    /**
     * The pr for a fetch operation
     * @param pr
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#pr(int) 
     */
    public CounterObject pr(int pr) {
        this.fetchMetaBuilder.pr(pr);
        return this;
    }

    /**
     * The pr for a fetch operation
     * @param pr
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#pr(com.basho.riak.client.cap.Quora) 
     */
    public CounterObject pr(Quora pr) {
        this.fetchMetaBuilder.pr(pr);
        return this;
    }
    
    /**
     * The pr for a fetch operation
     * @param pr
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#pr(Quorum)
     */
    public CounterObject pr(Quorum pr) {
        this.fetchMetaBuilder.pr(pr);
        return this;
    }
    
    
    /**
     * if notfound_ok counts towards r count (for a fetch operation)
     * 
     * @param notFoundOK
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#notFoundOK(boolean) 
     */
    public CounterObject notFoundOK(boolean notFoundOK) {
        this.fetchMetaBuilder.notFoundOK(notFoundOK);
        return this;
    }

    /**
     * fail early if a quorum of error/notfounds are reached before a successful
     * read (for a fetch operation)
     * 
     * @param basicQuorum
     * @return this
     * @see com.basho.riak.client.raw.FetchMeta.Builder#basicQuorum(boolean)
     */
    public CounterObject basicQuorum(boolean basicQuorum) {
        this.fetchMetaBuilder.basicQuorum(basicQuorum);
        return this;
    }
    
    
    /**
     * Execute this counter operation
     * @return The value of the counter or null if the counter does not exist or an increment
     *  is performed has not specified to return the new value via {@link CounterObject#returnValue(boolean) }
     * @throws RiakException 
     */
    public Long execute() throws RiakException
    {
        try 
        {
            if (increment != null)
            {
                return client.incrementCounter(bucket, counter, increment, storeMetaBuilder.build());
            }
            else
            {
                return client.fetchCounter(bucket, counter, fetchMetaBuilder.build());
            }
        }
        catch (IOException e)
        {
            throw new RiakException(e);
        }
    }
    
}
