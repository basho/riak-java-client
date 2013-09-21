/*
 * Copyright 2013 Basho Technologies Inc.
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

import com.basho.riak.client.FetchMeta;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.util.ByteArrayWrapper;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @author Russel Brown <russelldb at basho dot com>
 * @since 1.0
 */
public class FetchObject<T> implements ClientOperation<T>
{
    private final RiakCluster riakCluster;
    private final ByteArrayWrapper bucket;
    private final ByteArrayWrapper key;
    private final FetchMeta.Builder fetchMetaBuilder = new FetchMeta.Builder();
    private ConflictResolver<T> conflictResolver;
    private Converter<T> domainObjectConverter;
    
    public FetchObject(RiakCluster cluster, ByteArrayWrapper bucket, ByteArrayWrapper key)
    {
        this.bucket = bucket;
        this.key = key;
        this.riakCluster = cluster;
    }
    
    @Override
    public T execute() throws InterruptedException, ExecutionException
    {
        FetchOperation<T> fetchOperation = new FetchOperation<T>(bucket, key)
                                                .withConverter(domainObjectConverter)
                                                .withResolver(conflictResolver)
                                                .withFetchMeta(fetchMetaBuilder.build());
        riakCluster.execute(fetchOperation);
        return fetchOperation.get(); 
    }
    
    @Override
    public RiakFuture<T> executeAsync()
    {
        FetchOperation<T> fetchOperation = new FetchOperation<T>(bucket, key)
                                                .withConverter(domainObjectConverter)
                                                .withResolver(conflictResolver)
                                                .withFetchMeta(fetchMetaBuilder.build());
        riakCluster.execute(fetchOperation);
        return fetchOperation;
    }
    
    /**
     * Sets the {@link ConflictResolver} to be used if Riak returns siblings
     * @param conflictResolver
     * @return this
     */
    public FetchObject<T> withResolver(ConflictResolver<T> conflictResolver)
    {
        this.conflictResolver = conflictResolver;
        return this;
    }

    /**
     * A {@link Converter} to use to convert the data fetched to some other type
     *
     * @param domainObjectConverter The converter to use
     * @return this
     */
    public FetchObject<T> withConverter(Converter<T> domainObjectConverter)
    {
        this.domainObjectConverter = domainObjectConverter;
        return this;
    }
    
    /**
     * The read quorum for this fetch operation
     *
     * @param r an Integer for the read quorum
     * @return this
     */
    public FetchObject<T> r(int r)
    {
        fetchMetaBuilder.r(r);
        return this;
    }

    /**
     * The read quorum for this fetch operation
     *
     * @param r an Quora for the read quorum
     * @return this
     */
    public FetchObject<T> r(Quora r)
    {
        fetchMetaBuilder.r(r);
        return this;
    }

    /**
     * The read quorum for this fetch operation
     *
     * @param r an Quorum for the read quorum
     * @return this
     */
    public FetchObject<T> r(Quorum r)
    {
        fetchMetaBuilder.r(r);
        return this;
    }

    /**
     * @param pr
     * @return this
     * @see com.basho.riak.client.FetchMeta.Builder#pr(int)
     */
    public FetchObject<T> pr(int pr)
    {
        fetchMetaBuilder.pr(pr);
        return this;
    }

    /**
     * @param pr
     * @return this
     * @see com.basho.riak.client.FetchMeta.Builder#pr(Quora)
     */
    public FetchObject<T> pr(Quora pr)
    {
        fetchMetaBuilder.pr(pr);
        return this;
    }

    /**
     * @param pr
     * @return this
     * @see com.basho.riak.client.FetchMeta.Builder#pr(Quora)
     */
    public FetchObject<T> pr(Quorum pr)
    {
        fetchMetaBuilder.pr(pr);
        return this;
    }

    /**
     * @param notFoundOK
     * @return this
     * @see com.basho.riak.client.FetchMeta.Builder#notFoundOK(boolean)
     */
    public FetchObject<T> notFoundOK(boolean notFoundOK)
    {
        fetchMetaBuilder.notFoundOK(notFoundOK);
        return this;
    }

    /**
     * @param basicQuorum
     * @return this
     * @see com.basho.riak.client.FetchMeta.Builder#basicQuorum(boolean)
     */
    public FetchObject<T> basicQuorum(boolean basicQuorum)
    {
        fetchMetaBuilder.basicQuorum(basicQuorum);
        return this;
    }

    /**
     * @param returnDeletedVClock
     * @return this
     * @see com.basho.riak.client.FetchMeta.Builder#returnDeletedVClock(boolean)
     */
    public FetchObject<T> returnDeletedVClock(boolean returnDeletedVClock)
    {
        fetchMetaBuilder.returnDeletedVClock(returnDeletedVClock);
        return this;
    }

    // TODO: Make this work in PB?
//    /**
//     * Causes this fetch operation to be conditional on the supplied date.
//     * <p>
//     * <B>IMPORTANT NOTE:</B> This is an HTTP API specific conditional. If the client
//     * has not been configured to support the HTTP protocol the result of this 
//     * operation will be an exception. 
//     * </p>
//     * <p>
//     * If the object in Riak has not been modified since the supplied date the 
//     * value will not be returned.
//     * </p>
//     * @see com.basho.riak.client.RiakObject#isModified() 
//     * @param modifiedSince a last modified date.
//     *
//     * @return this
//     */
//    public FetchObject<T> ifModifiedSince(Date modifiedSince)
//    {
//        fetchMetaBuilder.modifiedSince(modifiedSince);
//        fetchMetaBuilder.vclock(null);
//        return this;
//    }

    /**
     * Causes this fetch operation to be conditional on the supplied vector clock.
     * <p>
     * <B>IMPORTANT NOTE:</B> This is an Protocol Buffers API specific conditional. 
     * If the client has not been configured to support the PB protocol the result of this 
     * operation will be an exception. 
     * </p>
     * <p>
     * If the object in Riak has not been modified since the supplied VClock the
     * value will not be returned.
     * </p>
     * @see com.basho.riak.client.query.RiakObject#isModified
     * @param vclock a vclock
     * @return this
     */
    public FetchObject<T> ifModifiedSince(VClock vclock)
    {
        fetchMetaBuilder.vclock(vclock);
        //fetchMetaBuilder.modifiedSince(null);
        return this;
    }

    /**
     * Causes the client to retrieve only the metadata and not the value of this
     * object.
     *
     * Note that if you are using HTTP and siblings are present the client does a
     * second get and retrieves all the values. This is due to how the HTTP API
     * handles siblings.
     *
     * Note: The {@link Converter} being used must be able to handle an empty
     * value.
     *
     * @return this
     * @see com.basho.riak.client.FetchMeta.Builder#headOnly(boolean
     * headOnly)
     */
    public FetchObject<T> headOnly()
    {
        fetchMetaBuilder.headOnly(true);
        return this;
    }

    public FetchObject<T> timeout(int timeout)
    {
        fetchMetaBuilder.timeout(timeout);
        return this;
    }

    public FetchObject<T> nval(int nval)
    {
        fetchMetaBuilder.nval(nval);
        return this;
    }

    public FetchObject<T> sloppyQuorum(boolean sloppyQuorum)
    {
        fetchMetaBuilder.sloppyQuorum(sloppyQuorum);
        return this;
    }
}
