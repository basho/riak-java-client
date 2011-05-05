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
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.operations.RiakOperation;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedFunction;
import com.basho.riak.client.raw.RawClient;

/**
 * @author russell
 * 
 */
public class WriteBucket implements RiakOperation<Bucket> {

    private final RawClient client;
    private Retrier retrier;
    private String name;

    private DefaultBucketProperties.Builder builder = new DefaultBucketProperties.Builder();

    public WriteBucket(final RawClient client, String name, final Retrier retrier) {
        this.name = name;
        this.client = client;
        this.retrier = retrier;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.RiakOperation#execute()
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

    public WriteBucket allowSiblings(boolean allowSiblings) {
        builder.allowSiblings(allowSiblings);
        return this;
    }

    public WriteBucket lastWriteWins(boolean lastWriteWins) {
        builder.lastWriteWins(lastWriteWins);
        return this;
    }

    public WriteBucket nVal(int nVal) {
        builder.nVal(nVal);
        return this;
    }

    public WriteBucket backend(String backend) {
        builder.backend(backend);
        return this;
    }

    public WriteBucket precommitHooks(Collection<NamedFunction> precommitHooks) {
        builder.precommitHooks(precommitHooks);
        return this;
    }

    public WriteBucket addPrecommitHook(NamedFunction preCommitHook) {
        builder.addPrecommitHook(preCommitHook);
        return this;
    }

    public WriteBucket postcommitHooks(Collection<NamedErlangFunction> postCommitHooks) {
        builder.postcommitHooks(postCommitHooks);
        return this;
    }

    public WriteBucket addPostcommitHook(NamedErlangFunction postcommitHook) {
        builder.addPostcommitHook(postcommitHook);
        return this;
    }

    public WriteBucket chashKeyFunction(NamedErlangFunction chashKeyFunction) {
        builder.chashKeyFunction(chashKeyFunction);
        return this;
    }

    public WriteBucket linkWalkFunction(NamedErlangFunction linkWalkFunction) {
        builder.linkWalkFunction(linkWalkFunction);
        return this;
    }

    public WriteBucket smallVClock(int smallVClock) {
        builder.smallVClock(smallVClock);
        return this;
    }

    public WriteBucket bigVClock(int bigVClock) {
        builder.bigVClock(bigVClock);
        return this;
    }

    public WriteBucket youngVClock(long youngVClock) {
        builder.youngVClock(youngVClock);
        return this;
    }

    public WriteBucket oldVClock(long oldVClock) {
        builder.oldVClock(oldVClock);
        return this;
    }

    public WriteBucket r(Quora r) {
        builder.r(r);
        return this;
    }

    public WriteBucket r(int r) {
        builder.r(r);
        return this;
    }

    public WriteBucket w(Quora w) {
        builder.w(w);
        return this;
    }

    public WriteBucket w(int w) {
        builder.w(w);
        return this;
    }

    public WriteBucket rw(Quora rw) {
        builder.rw(rw);
        return this;
    }

    public WriteBucket rw(int rw) {
        builder.rw(rw);
        return this;
    }

    public WriteBucket dw(Quora dw) {
        builder.dw(dw);
        return this;
    }

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

}
