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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;

import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.operations.RiakOperation;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedFunction;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.Transport;

/**
 * A {@link RiakOperation} for creating/updating a {@link Bucket}.
 * 
 * <p>
 * This class is a fluid builder for creating a {@link RiakOperation} that sets
 * bucket properties on a bucket in Riak. It delegates to a
 * {@link BucketPropertiesBuilder} then uses its {@link RawClient} and
 * {@link Retrier} to set the bucket properties in Riak.
 * </p>
 * <p>
 * NOTE: all the parameters on the builder are optional. If omitted then the
 * Riak defaults will be used. Also, very few of these properties are supported
 * by both underlying APIs at present. They are here for completeness sake.
 * Changes are underway to support all the properties. Check the docs for the
 * individual parameters to see what is supported.
 * </p>
 * 
 * @author russell
 * 
 */
public class WriteBucket implements RiakOperation<Bucket> {

    private final RawClient client;
    private Retrier retrier;
    private String name;
    private Collection<NamedFunction> precommitHooks;
    private Collection<NamedErlangFunction> postcommitHooks;

    private BucketPropertiesBuilder builder = new BucketPropertiesBuilder();
    private boolean lazyLoadProperties = false;
    
    /**
     * Create WriteBucket operation that delegates to the given {@link RawClient} via the give {@link Retrier}.
     * @param client the {@link RawClient} to delegate to
     * @param name the name of the bucket to create/update
     * @param retrier the {@link Retrier} to use
     */
    public WriteBucket(final RawClient client, String name, final Retrier retrier) {
        this.name = name;
        this.client = client;
        this.retrier = retrier;
    }

    /**
     * Create WriteBucket operation that delegates to the given
     * {@link RawClient} via the give {@link Retrier}.
     * 
     * @param client
     *            the {@link RawClient} to delegate to
     * @param bucket
     *            the bucket to update
     * @param retrier
     *            the {@link Retrier} to use
     */
    public WriteBucket(final RawClient client, Bucket bucket, final Retrier retrier) {
        this.name = bucket.getName();
        this.client = client;
        this.retrier = retrier;
        this.precommitHooks = bucket.getPrecommitHooks();
        this.postcommitHooks = bucket.getPostcommitHooks();
        
        // replace the default builder with one using the properties from the 
        // bucket that was passed in.
        builder = BucketPropertiesBuilder.from(bucket);
    }

    /**
     * Creates/updates a Bucket in Riak with the set of properties configured.
     * @return the {@link Bucket}
     */
    public Bucket execute() throws RiakRetryFailedException {
        final BucketProperties propsToStore = builder.precommitHooks(precommitHooks).postcommitHooks(postcommitHooks).build();

        retrier.attempt(new Callable<Void>() {
            public Void call() throws Exception {
                client.updateBucket(name, propsToStore);
                return null;
            }
        });

        BucketProperties properties;
        
        if (!lazyLoadProperties) {
            properties = retrier.attempt(new Callable<BucketProperties>() {
                public BucketProperties call() throws Exception {
                    return client.fetchBucket(name);
                }
            });
        } else {
            properties = new LazyBucketProperties(client, retrier, name);
        }
            

        return new DefaultBucket(name, properties, client, retrier);
    }

    /**
     * Should the bucket have allow_mult set to true?
     * @param allowSiblings
     * @return this
     */
    public WriteBucket allowSiblings(boolean allowSiblings) {
        builder.allowSiblings(allowSiblings);
        return this;
    }

    /**
     * Is this bucket last_write_wins?
     * @param lastWriteWins
     * @return this
     */
    public WriteBucket lastWriteWins(boolean lastWriteWins) {
        builder.lastWriteWins(lastWriteWins);
        return this;
    }

    /**
     * The n_val for this bucket
     * @param nVal
     * @return this
     */
    public WriteBucket nVal(int nVal) {
        builder.nVal(nVal);
        return this;
    }

    /**
     * Which backend this bucket uses.
     * @param backend
     * @return this
     */
    public WriteBucket backend(String backend) {
        builder.backend(backend);
        return this;
    }

    /**
     * A Collection of precommit hooks for this bucket
     * @param precommitHooks
     * @return
     */
    public WriteBucket precommitHooks(Collection<NamedFunction> precommitHooks) {
        builder.precommitHooks(precommitHooks);
        return this;
    }

    /**
     * Add a precommit hook to the Collection of hooks to be written.
     * @param preCommitHook
     * @return this
     */
    public WriteBucket addPrecommitHook(NamedFunction preCommitHook) {
        if(preCommitHook != null) {
            if(precommitHooks == null) {
                precommitHooks = new ArrayList<NamedFunction>();
            }
            precommitHooks.add(preCommitHook);
        }
        return this;
    }

    /**
     * Add a collection of postcommit hooks to the bucket to be written.
     * @param postCommitHooks
     * @return
     */
    public WriteBucket postcommitHooks(Collection<NamedErlangFunction> postCommitHooks) {
        builder.postcommitHooks(postCommitHooks);
        return this;
    }

    /**
     * Add a postcommit hook to the Collection of post commit hooks for the bucket to written.
     * NOTE: at present this is not supported by PB API and 
     * an {@link UnsupportedPropertyException} will be thrown if called for that transport
     * @param postcommitHook
     * @return
     */
    public WriteBucket addPostcommitHook(NamedErlangFunction postcommitHook) {
        if(postcommitHook != null) {
            if(postcommitHooks == null) {
                postcommitHooks = new ArrayList<NamedErlangFunction>();
            }
            postcommitHooks.add(postcommitHook);
        }
        return this;
    }

    /**
     * Set the chash_key_fun on the bucket to be written
     * @param chashKeyFunction
     * @return
     */
    public WriteBucket chashKeyFunction(NamedErlangFunction chashKeyFunction) {
        builder.chashKeyFunction(chashKeyFunction);
        return this;
    }

    /**
     * Set the link_walk_fun used by Riak on the bucket to be written.
     * @param linkWalkFunction
     * @return
     */
    public WriteBucket linkWalkFunction(NamedErlangFunction linkWalkFunction) {
        builder.linkWalkFunction(linkWalkFunction);
        return this;
    }

    /**
     * set the small vclock prune size
     * @param smallVClock
     * @return
     */
    public WriteBucket smallVClock(int smallVClock) {
        builder.smallVClock(smallVClock);
        return this;
    }

    /**
     * set the big_vclock prune size
     * @param bigVClock
     * @return
     */
    public WriteBucket bigVClock(int bigVClock) {
        builder.bigVClock(bigVClock);
        return this;
    }

    /**
     * set the young_vclock prune age
     * @param youngVClock
     * @return
     */
    public WriteBucket youngVClock(long youngVClock) {
        builder.youngVClock(youngVClock);
        return this;
    }

    /**
     * set the old_vclock prune age
     * @param oldVClock
     * @return
     */
    public WriteBucket oldVClock(long oldVClock) {
        builder.oldVClock(oldVClock);
        return this;
    }

    /**
     * The default r Quorom for the bucket
     * @param r
     * @return
     */
    public WriteBucket r(Quora r) {
        builder.r(r);
        return this;
    }

    /**
     * The default r quorom as an int
     * @param r
     * @return
     */
    public WriteBucket r(int r) {
        builder.r(r);
        return this;
    }

    /**
     * The default w quorom
     * @param w
     * @return
     */
    public WriteBucket w(Quora w) {
        builder.w(w);
        return this;
    }

    /**
     * The default w quorom as an int
     * @param w
     * @return
     */
    public WriteBucket w(int w) {
        builder.w(w);
        return this;
    }

    /**
     * The default rw quorom
     * @param rw
     * @return
     */
    public WriteBucket rw(Quora rw) {
        builder.rw(rw);
        return this;
    }

    /**
     * The default rw quorom as an int
     * @param rw
     * @return
     */
    public WriteBucket rw(int rw) {
        builder.rw(rw);
        return this;
    }

    /**
     * The default dw quorom
     * @param dw
     * @return
     */
    public WriteBucket dw(Quora dw) {
        builder.dw(dw);
        return this;
    }

    /**
     * The default dw quorom as an int
     * @param dw
     * @return
     */
    public WriteBucket dw(int dw) {
        builder.dw(dw);
        return this;
    }

    /**
     * The default pr quorom
     * @param pr
     * @return
     */
    public WriteBucket pr(Quora pr) {
        builder.pr(pr);
        return this;
    }

    /**
     * The default pr quorom as an int
     * @param pr
     * @return
     */
    public WriteBucket pr(int pr) {
        builder.pr(pr);
        return this;
    }

    /**
     * The default pw quorom
     * @param pw
     * @return
     */
    public WriteBucket pw(Quora pw) {
        builder.pw(pw);
        return this;
    }

    /**
     * The default dw quorom as an int
     * 
     * @param dw
     * @return
     */
    public WriteBucket pw(int pw) {
        builder.pw(pw);
        return this;
    }

    /**
     * The default basic_quorum value
     * NOTE: at present this is not supported by PB API and 
     * an {@link UnsupportedPropertyException} will be thrown if called for that transport 
     * @param basicQuorum
     * @return
     */
    public WriteBucket basicQuorum(boolean basicQuorum) {
        builder.basicQuorum(basicQuorum);
        return this;
    }

    /**
     * The default notfound_ok value
     * 
     * @param notFoundOK
     * @return
     */
    public WriteBucket notFoundOK(boolean notFoundOK) {
        builder.notFoundOK(notFoundOK);
        return this;
    }

    /**
     * Specify the retrier to use for this operation.
     * If non-provided will use the client configured default.
     *
     * @param retrier a Retrier to use for the execute operation
     * @return this
     */
    public WriteBucket withRetrier(final Retrier retrier) {
        this.retrier = retrier;
        return this;
    }

    /**
     * convenience for setting search=true **and** adding the search precommit
     * hook (support for both pre-1.0 and 1.0 search)
     * 
     * @return this
     */
    public WriteBucket enableForSearch() {
        addPrecommitHook(NamedErlangFunction.SEARCH_PRECOMMIT_HOOK);
        builder.search(true);
        return this;
    }

    /**
     * convenience for setting search=false **and** removing the search
     * precommit hook (support for both pre-1.0 and 1.0 search) 
     * 
     * @return this
     */
    public WriteBucket disableSearch() {
        if (precommitHooks != null) {
            precommitHooks.remove(NamedErlangFunction.SEARCH_PRECOMMIT_HOOK);
        }
        builder.search(false);
        return this;
    }

    /**
     * Prior to the addition of this method there was no way to prevent 
     * {@link #execute() } from fetching the {@link BucketProperties} from Riak
     * after storing any modifications made via this object. 
     * <p>
     * Calling this prior to {@link #execute() } allows you to defer fetching 
     * the bucket properties for this bucket from Riak
     * until they are required by one of the {@link Bucket} methods that
     * accesses them (e.g. {@link Bucket#getR() } ). If none of those methods are
     * called then they are never retrieved.
     * </p>
     * @return this 
     * @since 1.0.4
     */
    public WriteBucket lazyLoadBucketProperties() {
        this.lazyLoadProperties = true;
        return this;
    }
}
