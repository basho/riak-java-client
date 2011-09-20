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

import java.util.Collection;
import java.util.concurrent.Callable;

import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.operations.RiakOperation;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedFunction;
import com.basho.riak.client.raw.RawClient;

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
 * by either underlying API at present. They are here for completeness sake.
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

    private BucketPropertiesBuilder builder = new BucketPropertiesBuilder();

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
     * Creates/updates a Bucket in Riak with the set of properties configured.
     * @return the {@link Bucket}
     */
    public Bucket execute() throws RiakRetryFailedException {
        final BucketProperties propsToStore = builder.build();

        retrier.attempt(new Callable<Void>() {
            public Void call() throws Exception {
                client.updateBucket(name, propsToStore);
                return null;
            }
        });

        BucketProperties properties = retrier.attempt(new Callable<BucketProperties>() {
            public BucketProperties call() throws Exception {
                return client.fetchBucket(name);
            }
        });

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
     * NOTE: at present this is not supported so has no effect.
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
     * NOTE: at present this is not supported so has no effect.
     * @param backend
     * @return this
     */
    public WriteBucket backend(String backend) {
        builder.backend(backend);
        return this;
    }

    /**
     * A Collection of precommit hooks for this bucket
     * NOTE: at present this is not supported so has no effect.
     * @param precommitHooks
     * @return
     */
    public WriteBucket precommitHooks(Collection<NamedFunction> precommitHooks) {
        builder.precommitHooks(precommitHooks);
        return this;
    }

    /**
     * Add a precommit hook to the Collection of hooks to be written.
     * NOTE: at present this is not supported so has no effect.
     * @param preCommitHook
     * @return this
     */
    public WriteBucket addPrecommitHook(NamedFunction preCommitHook) {
        builder.addPrecommitHook(preCommitHook);
        return this;
    }

    /**
     * Add a collection of postcommit hooks to the bucket to be written.
     * NOTE: at present this is not supported so has no effect.
     * @param postCommitHooks
     * @return
     */
    public WriteBucket postcommitHooks(Collection<NamedErlangFunction> postCommitHooks) {
        builder.postcommitHooks(postCommitHooks);
        return this;
    }

    /**
     * Add a postcommit hook to the Collection of post commit hooks for the bucket to written.
     * NOTE: at present this is not supported so has no effect.
     * @param postcommitHook
     * @return
     */
    public WriteBucket addPostcommitHook(NamedErlangFunction postcommitHook) {
        builder.addPostcommitHook(postcommitHook);
        return this;
    }

    /**
     * Set the chash_key_fun on the bucket to be written
     * NOTE: at present this is not supported by the PB API and has no effect for that client.
     * @param chashKeyFunction
     * @return
     */
    public WriteBucket chashKeyFunction(NamedErlangFunction chashKeyFunction) {
        builder.chashKeyFunction(chashKeyFunction);
        return this;
    }

    /**
     * Set the link_walk_fun used by Riak on the bucket to be written.
     * NOTE: at present this is not supported by the PB API and has no effect for that client.
     * @param linkWalkFunction
     * @return
     */
    public WriteBucket linkWalkFunction(NamedErlangFunction linkWalkFunction) {
        builder.linkWalkFunction(linkWalkFunction);
        return this;
    }

    /**
     * set the small vclock prune size
     * NOTE: at present this is not supported so has no effect.
     * @param smallVClock
     * @return
     */
    public WriteBucket smallVClock(int smallVClock) {
        builder.smallVClock(smallVClock);
        return this;
    }

    /**
     * set the big_vclock prune size
     * NOTE: at present this is not supported so has no effect.
     * @param bigVClock
     * @return
     */
    public WriteBucket bigVClock(int bigVClock) {
        builder.bigVClock(bigVClock);
        return this;
    }

    /**
     * set the young_vclock prune age
     * NOTE: at present this is not supported so has no effect.
     * @param youngVClock
     * @return
     */
    public WriteBucket youngVClock(long youngVClock) {
        builder.youngVClock(youngVClock);
        return this;
    }

    /**
     * set the old_vclock prune age
     * NOTE: at present this is not supported so has no effect.
     * @param oldVClock
     * @return
     */
    public WriteBucket oldVClock(long oldVClock) {
        builder.oldVClock(oldVClock);
        return this;
    }

    /**
     * The default r Quorom for the bucket
     * NOTE: at present this is not supported so has no effect.
     * @param r
     * @return
     */
    public WriteBucket r(Quora r) {
        builder.r(r);
        return this;
    }

    /**
     * The default r quorom as an int
     * NOTE: at present this is not supported so has no effect.
     * @param r
     * @return
     */
    public WriteBucket r(int r) {
        builder.r(r);
        return this;
    }

    /**
     * The default w quorom
     * NOTE: at present this is not supported so has no effect.
     * @param w
     * @return
     */
    public WriteBucket w(Quora w) {
        builder.w(w);
        return this;
    }

    /**
     * The default w quorom as an int
     * NOTE: at present this is not supported so has no effect.
     * @param w
     * @return
     */
    public WriteBucket w(int w) {
        builder.w(w);
        return this;
    }

    /**
     * The default rw quorom
     * NOTE: at present this is not supported so has no effect.
     * @param rw
     * @return
     */
    public WriteBucket rw(Quora rw) {
        builder.rw(rw);
        return this;
    }

    /**
     * The default rw quorom as an int
     * NOTE: at present this is not supported so has no effect.
     * @param rw
     * @return
     */
    public WriteBucket rw(int rw) {
        builder.rw(rw);
        return this;
    }

    /**
     * The default dw quorom
     * NOTE: at present this is not supported so has no effect.
     * @param dw
     * @return
     */
    public WriteBucket dw(Quora dw) {
        builder.dw(dw);
        return this;
    }

    /**
     * The default dw quorom as an int
     * NOTE: at present this is not supported so has no effect.
     * @param dw
     * @return
     */
    public WriteBucket dw(int dw) {
        builder.dw(dw);
        return this;
    }

    /**
     * Specify the retrier to use for this operation.
     * If non-provided will use the client configured default.
     *
     * @param retrier a Retrier to use for the execute operation
     * @return this
     */
    public WriteBucket retrier(final Retrier retrier) {
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
        builder.addPrecommitHook(NamedErlangFunction.SEARCH_PRECOMMIT_HOOK).search(true);
        return this;
    }
}
