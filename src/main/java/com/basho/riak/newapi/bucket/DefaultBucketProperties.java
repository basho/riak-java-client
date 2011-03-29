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

import java.util.ArrayList;
import java.util.Collection;

import com.basho.riak.newapi.cap.CAP;
import com.basho.riak.newapi.cap.Quorum;
import com.basho.riak.newapi.query.NamedErlangFunction;
import com.basho.riak.newapi.query.NamedFunction;

/**
 * Since not all interfaces to Riak are equal in terms of what they provide not
 * all RawClients can be expected to set all values. Which means that *any* of
 * the getters may return null.
 * 
 * @author russell
 */
public class DefaultBucketProperties implements BucketProperties {

    private final Boolean allowSiblings;
    private final Boolean lastWriteWins;
    private final Integer nVal;
    private final String backend;
    private final Integer smallVClock;
    private final Integer bigVClock;
    private final Long youngVClock;
    private final Long oldVClock;
    private final Collection<NamedFunction> precommitHooks;
    private final Collection<NamedErlangFunction> postcommitHooks;
    private final Quorum r;
    private final Quorum w;
    private final Quorum dw;
    private final Quorum rw;
    private final NamedErlangFunction chashKeyFunction;
    private final NamedErlangFunction linkWalkFunction;

    /**
     * @param allowSiblings
     * @param lastWriteWins
     * @param nVal
     * @param backend
     * @param smallVClock
     * @param bigVClock
     * @param youngVClock
     * @param oldVClock
     * @param precommitHooks
     * @param postcommitHooks
     * @param r
     * @param w
     * @param dw
     * @param rw
     * @param chashKeyFunction
     * @param linkWalkFunction
     */
    private DefaultBucketProperties(Builder builder) {
        this.allowSiblings = builder.allowSiblings;
        this.lastWriteWins = builder.lastWriteWins;
        this.nVal = builder.nVal;
        this.backend = builder.backend;
        this.smallVClock = builder.smallVClock;
        this.bigVClock = builder.bigVClock;
        this.youngVClock = builder.youngVClock;
        this.oldVClock = builder.oldVClock;
        this.precommitHooks = builder.precommitHooks;
        this.postcommitHooks = builder.postcommitHooks;
        this.r = builder.r;
        this.w = builder.w;
        this.dw = builder.dw;
        this.rw = builder.rw;
        this.chashKeyFunction = builder.chashKeyFunction;
        this.linkWalkFunction = builder.linkWalkFunction;
    }

    /**
     * @return the allowSiblings if set, or null if not
     */
    public Boolean getAllowSiblings() {
        return allowSiblings;
    }

    /**
     * @return the lastWriteWins if set or null if not
     */
    public Boolean getLastWriteWins() {
        return lastWriteWins;
    }

    /**
     * @return the nVal if set or null if not
     */
    public Integer getNVal() {
        return nVal;
    }

    /**
     * @return the backend if set, or null.
     */
    public String getBackend() {
        return backend;
    }

    /**
     * 
     * @return the small vclock pruning property if set, or null.
     */
    public int getSmallVClock() {
        return smallVClock;
    }

    /**
     * 
     * @return the big vclock pruning size property if set, or null.
     */
    public int getBigVClock() {
        return bigVClock;
    }

    /**
     * 
     * @return the young vclock prune property if set, or null.
     */
    public long getYoungVClock() {
        return youngVClock;
    }

    /**
     * 
     * @return the old vclock prune property if set, or null
     */
    public long getOldVClock() {
        return oldVClock;
    }

    /**
     * @return the pre commit hooks, if any, or an empty collection.
     */
    public Collection<NamedFunction> getPrecommitHooks() {
        return precommitHooks;
    }

    /**
     * @return the post commit hooks, if ant, or an empty collection.
     */
    public Collection<NamedErlangFunction> getPostcommitHooks() {
        return postcommitHooks;
    }

    /**
     * 
     * @return the default CAP read quorum for this bucket, or null.
     */
    public Quorum getR() {
        return r;
    }

    /**
     * 
     * @return the default CAP write quorum for this bucket, or null.
     */
    public Quorum getW() {
        return w;
    }

    /**
     * 
     * @return the default CAP RW (delete) quorum for this bucket, or null.
     */
    public Quorum getRW() {
        return rw;
    }

    /**
     * 
     * @return the default CAP durable write quorum for this bucket, or null.
     */
    public Quorum getDW() {
        return dw;
    }

    /**
     * @return the key hashing function for the bucket, or null.
     */
    public NamedErlangFunction getChashKeyFunction() {
        return chashKeyFunction;
    }

    /**
     * @return the link walking function for the bucket, or null.
     */
    public NamedErlangFunction getLinkWalkFunction() {
        return linkWalkFunction;
    }

    /**
     * 
     * @return a Builder populated from this BucketProperties' values.
     */
    public DefaultBucketProperties.Builder fromMe() {
        return DefaultBucketProperties.from(this);
    }

    /**
     * 
     * @param properties
     * @return a Builder populated with properties values.
     */
    public static DefaultBucketProperties.Builder from(DefaultBucketProperties properties) {
        return DefaultBucketProperties.Builder.from(properties);
    }

    /**
     * Use to create instances of BucketProperties.
     * 
     * @author russell
     * 
     */
    public static final class Builder {

        public NamedErlangFunction linkWalkFunction;
        public NamedErlangFunction chashKeyFunction;
        public Quorum rw;
        public Quorum dw;
        public Quorum w;
        public Quorum r;
        public Collection<NamedErlangFunction> postcommitHooks = new ArrayList<NamedErlangFunction>();
        public Collection<NamedFunction> precommitHooks = new ArrayList<NamedFunction>();
        public Long oldVClock;
        public Long youngVClock;
        public Integer bigVClock;
        public Integer smallVClock;
        public String backend;
        public int nVal;
        public Boolean lastWriteWins;
        public Boolean allowSiblings;

        public BucketProperties build() {
            return new DefaultBucketProperties(this);
        }

        /**
         * @param p
         *            the BucketProperties to copy to the builder
         * @return a builder with all values set from p
         */
        public static Builder from(DefaultBucketProperties p) {
            Builder b = new Builder();
            b.allowSiblings = p.getAllowSiblings();
            b.lastWriteWins = p.getLastWriteWins();
            b.nVal = p.getNVal();
            b.backend = p.getBackend();
            b.smallVClock = p.getSmallVClock();
            b.bigVClock = p.getBigVClock();
            b.youngVClock = p.getYoungVClock();
            b.oldVClock = p.getOldVClock();
            b.postcommitHooks.addAll(p.getPostcommitHooks());
            b.precommitHooks.addAll(p.getPrecommitHooks());
            b.r = p.getR();
            b.w = p.getW();
            b.dw = p.getDW();
            b.rw = p.getRW();
            b.chashKeyFunction = p.getChashKeyFunction();
            b.linkWalkFunction = p.getLinkWalkFunction();
            return b;
        }

        public Builder allowSiblings(boolean allowSiblings) {
            this.allowSiblings = allowSiblings;
            return this;
        }

        public Builder lastWriteWins(boolean lastWriteWins) {
            this.lastWriteWins = lastWriteWins;
            return this;
        }

        public Builder nVal(int nVal) {
            this.nVal = nVal;
            return this;
        }

        public Builder backend(String backend) {
            this.backend = backend;
            return this;
        }

        public Builder precommitHooks(Collection<NamedFunction> precommitHooks) {
            this.precommitHooks = new ArrayList<NamedFunction>(precommitHooks);
            return this;
        }

        public Builder addPrecommitHook(NamedFunction preCommitHook) {
            if (this.precommitHooks == null) {
                this.precommitHooks = new ArrayList<NamedFunction>();
            }
            this.precommitHooks.add(preCommitHook);
            return this;
        }

        public Builder postcommitHooks(Collection<NamedErlangFunction> postCommitHooks) {
            this.postcommitHooks = new ArrayList<NamedErlangFunction>(postCommitHooks);
            return this;
        }

        public Builder addPostcommitHook(NamedErlangFunction postcommitHook) {
            if (this.postcommitHooks == null) {
                this.postcommitHooks = new ArrayList<NamedErlangFunction>();
            }
            this.precommitHooks.add(postcommitHook);
            return this;
        }

        public Builder chashKeyFunction(NamedErlangFunction chashKeyFunction) {
            this.chashKeyFunction = chashKeyFunction;
            return this;
        }

        public Builder linkWalkFunction(NamedErlangFunction linkWalkFunction) {
            this.linkWalkFunction = linkWalkFunction;
            return this;
        }

        /**
         * @param smallVClock
         * @return
         */
        public Builder smallVClock(int smallVClock) {
            this.smallVClock = smallVClock;
            return this;
        }

        /**
         * @param bigVClock
         * @return
         */
        public Builder bigVClock(int bigVClock) {
            this.bigVClock = bigVClock;
            return this;
        }

        /**
         * @param youngVClock
         * @return
         */
        public Builder youngVClock(long youngVClock) {
            this.youngVClock = youngVClock;
            return this;
        }

        /**
         * @param oldVClock
         * @return
         */
        public Builder oldVClock(long oldVClock) {
            this.oldVClock = oldVClock;
            return this;
        }

        /**
         * @param r
         * @return
         */
        public Builder r(CAP r) {
            this.r = new Quorum(r);
            return this;
        }

        public Builder r(int r) {
            this.r = new Quorum(r);
            return this;
        }

        /**
         * @param w
         * @return
         */
        public Builder w(CAP w) {
            this.w = new Quorum(w);
            return this;
        }

        public Builder w(int w) {
            this.w = new Quorum(w);
            return this;
        }

        /**
         * @param rw
         * @return
         */
        public Builder rw(CAP rw) {
            this.rw = new Quorum(rw);
            return this;
        }

        public Builder rw(int rw) {
            this.rw = new Quorum(rw);
            return this;
        }

        /**
         * @param dw
         * @return
         */
        public Builder dw(CAP dw) {
            this.dw = new Quorum(dw);
            return this;
        }

        public Builder dw(int dw) {
            this.dw = new Quorum(dw);
            return this;
        }

    }
}