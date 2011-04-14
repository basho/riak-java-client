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

import com.basho.riak.client.raw.Command;
import com.basho.riak.client.raw.DefaultRetrier;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.newapi.RiakRetryFailedException;
import com.basho.riak.newapi.bucket.DefaultBucketProperties.Builder;
import com.basho.riak.newapi.cap.CAP;
import com.basho.riak.newapi.operations.RiakOperation;
import com.basho.riak.newapi.query.NamedErlangFunction;
import com.basho.riak.newapi.query.NamedFunction;

/**
 * @author russell
 * 
 */
public class WriteBucket implements RiakOperation<Bucket> {

    private final RawClient client;
    private String name;

    private Builder builder = new Builder();
    private int retries = 0;

    public WriteBucket(final RawClient client, Bucket b) {
        this.name = b.getName();
        this.client = client;
    }

    public WriteBucket(final RawClient client, String name) {
        this.name = name;
        this.client = client;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.RiakOperation#execute()
     */
    public Bucket execute() throws RiakRetryFailedException {
        final BucketProperties propsToStore = builder.build();

        new DefaultRetrier().attempt(new Command<Void>() {
            public Void execute() throws IOException {
                client.updateBucket(name, propsToStore);
                return null;
            }
        }, retries);

        BucketProperties properties = new DefaultRetrier().attempt(new Command<BucketProperties>() {
            public BucketProperties execute() throws IOException {
                return client.fetchBucket(name);
            }
        }, retries);

        return new DefaultBucket(name, properties, client);
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

    public WriteBucket r(CAP r) {
        builder.r(r);
        return this;
    }

    public WriteBucket r(int r) {
        builder.r(r);
        return this;
    }

    public WriteBucket w(CAP w) {
        builder.w(w);
        return this;
    }

    public WriteBucket w(int w) {
        builder.w(w);
        return this;
    }

    public WriteBucket rw(CAP rw) {
        builder.rw(rw);
        return this;
    }

    public WriteBucket rw(int rw) {
        builder.rw(rw);
        return this;
    }

    public WriteBucket dw(CAP dw) {
        builder.dw(dw);
        return this;
    }

    public WriteBucket dw(int dw) {
        builder.dw(dw);
        return this;
    }

    public WriteBucket retry(int n) {
        this.retries = n;
        return this;
    }

}
