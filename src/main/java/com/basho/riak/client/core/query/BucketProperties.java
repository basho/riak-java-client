/*
 * Copyright 2013 Basho Technoilogies Inc.
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
package com.basho.riak.client.core.query;

import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.core.query.functions.Function;
import java.util.LinkedList;
import java.util.List;

/**
 * Bucket properties used for buckets and bucket types.
 *
 * Note that when instantiating a new instance there are no default values.
 * Calling a getter for a specific property will return null if it has not been
 * explicitly set. Unset properties are ignored when writing to a bucket or
 * bucket type in Riak.
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class BucketProperties
{
    private final Function linkwalkFunction;
    private final Function chashKeyFunction;
    private final Quorum rw;
    private final Quorum dw;
    private final Quorum w;
    private final Quorum r;
    private final Quorum pr;
    private final Quorum pw;
    private final Boolean notFoundOk;
    private final Boolean basicQuorum;
    private final List<Function> precommitHooks;
    private final List<Function> postcommitHooks;
    private final Long oldVClock;
    private final Long youngVClock;
    private final Long bigVClock;
    private final Long smallVClock;
    private final String backend;
    private final Integer nVal;
    private final Boolean lastWriteWins;
    private final Boolean allowSiblings;
    private final Boolean search;
    private final String yokozunaIndex;
    private final Integer hllPrecision;

    private BucketProperties(Builder builder)
    {
        this.allowSiblings = builder.allowSiblings;
        this.backend = builder.backend;
        this.basicQuorum = builder.basicQuorum;
        this.bigVClock = builder.bigVClock;
        this.chashKeyFunction = builder.chashKeyFunction;
        this.dw = builder.dw;
        this.lastWriteWins = builder.lastWriteWins;
        this.linkwalkFunction = builder.linkwalkFunction;
        this.nVal = builder.nVal;
        this.notFoundOk = builder.notFoundOk;
        this.oldVClock = builder.oldVClock;
        this.postcommitHooks = builder.postcommitHooks;
        this.pr = builder.pr;
        this.precommitHooks = builder.precommitHooks;
        this.pw = builder.pw;
        this.r = builder.r;
        this.rw = builder.rw;
        this.search = builder.search;
        this.smallVClock = builder.smallVClock;
        this.w = builder.w;
        this.yokozunaIndex = builder.yokozunaIndex;
        this.youngVClock = builder.youngVClock;
        this.hllPrecision = builder.hllPrecision;
    }

    /**
     * Determine if a linkfun value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasLinkwalkFunction()
    {
        return linkwalkFunction != null;
    }

    /**
     * Get the linkfun value.
     *
     * @return the link walking function for the bucket, or null.
     */
    public Function getLinkwalkFunction()
    {
        return linkwalkFunction;
    }

    /**
     * Determine if a chash_keyfun value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasChashKeyFunction()
    {
        return chashKeyFunction != null;
    }

    /**
     * Get the chash_keyfun value.
     *
     * @return the key hashing function for the bucket, or null.
     */
    public Function getChashKeyFunction()
    {
        return chashKeyFunction;
    }

    /**
     * Determine if an rw value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasRw()
    {
        return rw != null;
    }

    /**
     * Get the rw value.
     *
     * @return the rw value as a Quorum, or null if not set.
     */
    public Quorum getRw()
    {
        return rw;
    }

    /**
     * Determine if a dw value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasDw()
    {
        return rw != null;
    }

    /**
     * Get the dw value.
     *
     * @return the dw value as a Quorum, or null if not set.
     */
    public Quorum getDw()
    {
        return dw;
    }

    /**
     * Determine if a w value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasW()
    {
        return w != null;
    }

    /**
     * Get the w value.
     *
     * @return the w value as a Quorum, or null if not set.
     */
    public Quorum getW()
    {
        return w;
    }

    /**
     * Determine if an r value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasR()
    {
        return r != null;
    }

    /**
     * Get the r value.
     *
     * @return the r value as a Quorum, or null if not set.
     */
    public Quorum getR()
    {
        return this.r;
    }

    /**
     * Determine if a pr value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasPr()
    {
        return pr != null;
    }

    /**
     * Get the pr value.
     *
     * @return the pr value as a Quorum, or null.
     */
    public Quorum getPr()
    {
        return pr;
    }

    /**
     * Determine if a pw value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasPw()
    {
        return pw != null;
    }

    /**
     * Set the pw value.
     *
     * @return the pw value as a Quorum, or null.
     */
    public Quorum getPw()
    {
        return pw;
    }

    /**
     * Determine if a not_found_ok value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasNotFoundOk()
    {
        return notFoundOk != null;
    }

    /**
     * Get the not_found_ok value.
     *
     * @return the not_found_ok value or null if not set.
     * @see BucketProperties.Builder#withNotFoundOk(boolean)
     */
    public Boolean getNotFoundOk()
    {
        return notFoundOk;
    }

    /**
     * Determine if a basic_quorum value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasBasicQuorum()
    {
        return basicQuorum != null;
    }

    /**
     * Get the basic_quorum value.
     *
     * @return the basic_quorum value, or null if not set.
     * @see BucketProperties.Builder#withBasicQuorum(boolean)
     */
    public Boolean getBasicQuorum()
    {
        return basicQuorum;
    }

    /**
     * Determine if any pre-commit hooks have been added.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasPrecommitHooks()
    {
        return !precommitHooks.isEmpty();
    }

    /**
     * Get the list of pre-commit hooks.
     *
     * @return a List containing the pre-commit hooks.
     * @see
     * BucketProperties.Builder#withPrecommitHook(com.basho.riak.client.core.query.functions.Function)
     */
    public List<Function> getPrecommitHooks()
    {
        return precommitHooks;
    }

    /**
     * Determine if any post-commit hooks have been added.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasPostcommitHooks()
    {
        return !postcommitHooks.isEmpty();
    }

    /**
     * Get the list of post-commit hooks.
     *
     * @return a List containing the post-commit hooks
     * @see
     * BucketProperties.Builder#withPostcommitHook(com.basho.riak.client.core.query.functions.Function)
     */
    public List<Function> getPostcommitHooks()
    {
        return postcommitHooks;
    }

    /**
     * Determine if an old_vclock value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasOldVClock()
    {
        return oldVClock != null;
    }

    /**
     * Get the old_vclock value.
     *
     * @return the old_vclock value as a long or null if not set.
     * @see BucketProperties.Builder#withOldVClock(long)
     */
    public Long getOldVClock()
    {
        return oldVClock;
    }

    /**
     * Determine if an young_vclock value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasYoungVClock()
    {
        return youngVClock != null;
    }

    /**
     * Get the young_vclock value.
     *
     * @return the young_vclock value as an long or null if not set.
     * @see BucketProperties.Builder#withYoungVClock(long)
     */
    public Long getYoungVClock()
    {
        return youngVClock;
    }

    /**
     * Determine if an big_vclock value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasBigVClock()
    {
        return bigVClock != null;
    }

    /**
     * Get the big_vclock value.
     *
     * @return the big_vclock value as a long or null if not set.
     * @see BucketProperties.Builder#withBigVClock(long)
     */
    public Long getBigVClock()
    {
        return bigVClock;
    }

    /**
     * Determine if an small_vclock value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasSmallVClock()
    {
        return smallVClock != null;
    }

    /**
     * Get the small_vclock value.
     *
     * @return the small_vclock value as a long or null if not set.
     * @see BucketProperties.Builder#withSmallVClock(long)
     */
    public Long getSmallVClock()
    {
        return smallVClock;
    }

    /**
     * Determine if an backend value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasBackend()
    {
        return backend != null;
    }

    /**
     * Get the backend. Only applies when using {@code riak_kv_multi_backend} in
     * Riak.
     *
     * @return the name of the backend or null if not set.
     */
    public String getBackend()
    {
        return backend;
    }

    /**
     * Determine if an nVal value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasNVal()
    {
        return nVal != null;
    }

    /**
     * Get the nVal.
     *
     * @return the nVal value or null if not set.
     */
    public Integer getNVal()
    {
        return nVal;
    }

    /**
     * Determine if a last_write_wins value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasLastWriteWins()
    {
        return lastWriteWins != null;
    }

    /**
     * Get the last_write_wins value.
     *
     * @return the last_write_wins value or null if not set.
     */
    public Boolean getLastWriteWins()
    {
        return lastWriteWins;
    }

    /**
     * Determine if an allow_multi value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasAllowMulti()
    {
        return allowSiblings != null;
    }

    /**
     * Get the allow_multi value.
     *
     * @return the allow_multi value or null if not set.
     */
    public Boolean getAllowMulti()
    {
        return allowSiblings;
    }

    /**
     * Determine if legacy Riak Search has been enabled.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasLegacyRiakSearchEnabled()
    {
        return search != null;
    }

    /**
     * Determine if legacy Riak Search is enabled.
     *
     * @return true if Riak Search is enabled, false otherwise.
     */
    public Boolean getLegacyRiakSearchEnabled()
    {
        return search;
    }

    /**
     * Determine if a Yokozuna Index has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasSearchIndex()
    {
        return yokozunaIndex != null;
    }

    /**
     * Get the associated Yokozuna Index.
     *
     * @return the Yokozuna index name or null if not set.
     */
    public String getSearchIndex()
    {
        return yokozunaIndex;
    }

    /**
     * Determine if an hllPrecision value has been set.
     *
     * @return true if set, false otherwise.
     */
    public boolean hasHllPrecision()
    {
        return hllPrecision != null;
    }

    /**
     * Get the HyperLogLog Precision.
     *
     * @return the hllPrecision value or null if not set.
     */
    public Integer getHllPrecision()
    {
        return hllPrecision;
    }

    @Override
    public String toString()
    {
        return String.format("DefaultBucketProperties [allowSiblings=%s, "
            + "lastWriteWins=%s, nVal=%s, backend=%s,  "
            + "capProps=[rw=%s,dw=%s,w=%s,r=%s,pr=%s,pw=%s],"
            + "vclockProps=[oldVClock=%s, youngVClock=%s, bigVClock=%s, smallVClock=%s],"
            + "precommitHooks=%s, postcommitHooks=%s, "
            + ", chashKeyFunction=%s, linkWalkFunction=%s, search=%s,"
            + "yokozunaIndex=%s, hllPrecision=%s]",
                             allowSiblings, lastWriteWins, nVal, backend, rw, dw,
                             w, r, pr, pw, oldVClock, youngVClock, bigVClock, smallVClock,
                             precommitHooks, postcommitHooks,
                             chashKeyFunction, linkwalkFunction, search,
                             yokozunaIndex, hllPrecision);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        BucketProperties that = (BucketProperties) o;

        if (linkwalkFunction != null ? !linkwalkFunction.equals(that.linkwalkFunction) : that.linkwalkFunction != null)
        {
            return false;
        }
        if (chashKeyFunction != null ? !chashKeyFunction.equals(that.chashKeyFunction) : that.chashKeyFunction != null)
        {
            return false;
        }
        if (rw != null ? !rw.equals(that.rw) : that.rw != null)
        {
            return false;
        }
        if (dw != null ? !dw.equals(that.dw) : that.dw != null)
        {
            return false;
        }
        if (w != null ? !w.equals(that.w) : that.w != null)
        {
            return false;
        }
        if (r != null ? !r.equals(that.r) : that.r != null)
        {
            return false;
        }
        if (pr != null ? !pr.equals(that.pr) : that.pr != null)
        {
            return false;
        }
        if (pw != null ? !pw.equals(that.pw) : that.pw != null)
        {
            return false;
        }
        if (notFoundOk != null ? !notFoundOk.equals(that.notFoundOk) : that.notFoundOk != null)
        {
            return false;
        }
        if (basicQuorum != null ? !basicQuorum.equals(that.basicQuorum) : that.basicQuorum != null)
        {
            return false;
        }
        if (precommitHooks != null ? !precommitHooks.equals(that.precommitHooks) : that.precommitHooks != null)
        {
            return false;
        }
        if (postcommitHooks != null ? !postcommitHooks.equals(that.postcommitHooks) : that.postcommitHooks != null)
        {
            return false;
        }
        if (oldVClock != null ? !oldVClock.equals(that.oldVClock) : that.oldVClock != null)
        {
            return false;
        }
        if (youngVClock != null ? !youngVClock.equals(that.youngVClock) : that.youngVClock != null)
        {
            return false;
        }
        if (bigVClock != null ? !bigVClock.equals(that.bigVClock) : that.bigVClock != null)
        {
            return false;
        }
        if (smallVClock != null ? !smallVClock.equals(that.smallVClock) : that.smallVClock != null)
        {
            return false;
        }
        if (backend != null ? !backend.equals(that.backend) : that.backend != null)
        {
            return false;
        }
        if (nVal != null ? !nVal.equals(that.nVal) : that.nVal != null)
        {
            return false;
        }
        if (lastWriteWins != null ? !lastWriteWins.equals(that.lastWriteWins) : that.lastWriteWins != null)
        {
            return false;
        }
        if (allowSiblings != null ? !allowSiblings.equals(that.allowSiblings) : that.allowSiblings != null)
        {
            return false;
        }
        if (search != null ? !search.equals(that.search) : that.search != null)
        {
            return false;
        }
        if (yokozunaIndex != null ? !yokozunaIndex.equals(that.yokozunaIndex) : that.yokozunaIndex != null)
        {
            return false;
        }
        return hllPrecision != null ? hllPrecision.equals(that.hllPrecision) : that.hllPrecision == null;

    }

    @Override
    public int hashCode()
    {
        int result = linkwalkFunction != null ? linkwalkFunction.hashCode() : 0;
        result = 31 * result + (chashKeyFunction != null ? chashKeyFunction.hashCode() : 0);
        result = 31 * result + (rw != null ? rw.hashCode() : 0);
        result = 31 * result + (dw != null ? dw.hashCode() : 0);
        result = 31 * result + (w != null ? w.hashCode() : 0);
        result = 31 * result + (r != null ? r.hashCode() : 0);
        result = 31 * result + (pr != null ? pr.hashCode() : 0);
        result = 31 * result + (pw != null ? pw.hashCode() : 0);
        result = 31 * result + (notFoundOk != null ? notFoundOk.hashCode() : 0);
        result = 31 * result + (basicQuorum != null ? basicQuorum.hashCode() : 0);
        result = 31 * result + (precommitHooks != null ? precommitHooks.hashCode() : 0);
        result = 31 * result + (postcommitHooks != null ? postcommitHooks.hashCode() : 0);
        result = 31 * result + (oldVClock != null ? oldVClock.hashCode() : 0);
        result = 31 * result + (youngVClock != null ? youngVClock.hashCode() : 0);
        result = 31 * result + (bigVClock != null ? bigVClock.hashCode() : 0);
        result = 31 * result + (smallVClock != null ? smallVClock.hashCode() : 0);
        result = 31 * result + (backend != null ? backend.hashCode() : 0);
        result = 31 * result + (nVal != null ? nVal.hashCode() : 0);
        result = 31 * result + (lastWriteWins != null ? lastWriteWins.hashCode() : 0);
        result = 31 * result + (allowSiblings != null ? allowSiblings.hashCode() : 0);
        result = 31 * result + (search != null ? search.hashCode() : 0);
        result = 31 * result + (yokozunaIndex != null ? yokozunaIndex.hashCode() : 0);
        result = 31 * result + (hllPrecision != null ? hllPrecision.hashCode() : 0);
        return result;
    }

    public static class Builder
    {
        private Function linkwalkFunction;
        private Function chashKeyFunction;
        private Quorum rw;
        private Quorum dw;
        private Quorum w;
        private Quorum r;
        private Quorum pr;
        private Quorum pw;
        private Boolean notFoundOk;
        private Boolean basicQuorum;
        private final List<Function> precommitHooks = new LinkedList<>();
        private final List<Function> postcommitHooks = new LinkedList<>();
        private Long oldVClock;
        private Long youngVClock;
        private Long bigVClock;
        private Long smallVClock;
        private String backend;
        private Integer nVal;
        private Boolean lastWriteWins;
        private Boolean allowSiblings;
        private Boolean search;
        private String yokozunaIndex;
        private Integer hllPrecision;

        public Builder()
        {
        }

        /**
         * Set the allow_multi value.
         *
         * @param allow whether to allow sibling objects to be created.
         * @return a reference to this object.
         */
        public Builder withAllowMulti(boolean allow)
        {
            this.allowSiblings = allow;
            return this;
        }

        /**
         * Set the backend used by this bucket. Only applies when using
         * {@code riak_kv_multi_backend} in Riak.
         *
         * @param backend the name of the backend to use.
         * @return a reference to this object.
         */
        public Builder withBackend(String backend)
        {
            if (null == backend || backend.length() == 0)
            {
                throw new IllegalArgumentException("Backend can not be null or zero length");
            }
            this.backend = backend;
            return this;
        }

        /**
         * Set the basic_quorum value.
         *
         * The parameter controls whether a read request should return early in
         * some fail cases. E.g. If a quorum of nodes has already returned
         * notfound/error, don't wait around for the rest.
         *
         * @param use the basic_quorum value.
         * @return a reference to this object.
         */
        public Builder withBasicQuorum(boolean use)
        {
            this.basicQuorum = use;
            return this;
        }

        /**
         * Set the big_vclock value.
         *
         * @param bigVClock a long representing a epoch time value.
         * @return a reference to this object.
         * @see <a
         * href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector
         * Clock Pruning</a>
         */
        public Builder withBigVClock(long bigVClock)
        {
            this.bigVClock = bigVClock;
            return this;
        }

        /**
         * Set the chash_keyfun value.
         *
         * @param func a Function representing the Erlang func to use.
         * @return a reference to this object.
         */
        public Builder withChashkeyFunction(Function func)
        {
            verifyErlangFunc(func);
            this.chashKeyFunction = func;
            return this;
        }

        /**
         * Set the last_write_wins value. Unless you really know what you're
         * doing, you probably do not want to set this to true.
         *
         * @param wins whether to ignore vector clocks when writing.
         * @return a reference to this object.
         */
        public Builder withLastWriteWins(boolean wins)
        {
            this.lastWriteWins = wins;
            return this;
        }

        /**
         * Set the linkfun value.
         *
         * @param func a Function representing the Erlang func to use.
         * @return a reference to this object.
         */
        public Builder withLinkwalkFunction(Function func)
        {
            verifyErlangFunc(func);
            this.linkwalkFunction = func;
            return this;
        }

        /**
         * Set the rw value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param rw the rw value as an integer.
         * @return a reference to this object.
         */
        public Builder withRw(int rw)
        {
            this.rw = new Quorum(rw);
            return this;
        }

        /**
         * Set the rw value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param rw the rw value as a Quorum.
         * @return a reference to this object.
         */
        public Builder withRw(Quorum rw)
        {
            this.rw = rw;
            return this;
        }

        /**
         * Set the dw value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param dw the dw value as an integer.
         * @return a reference to this object.
         */
        public Builder withDw(int dw)
        {
            this.dw = new Quorum(dw);
            return this;
        }

        /**
         * Set the w value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param w the w value as an integer.
         * @return a reference to this object.
         */
        public Builder withW(int w)
        {
            this.w = new Quorum(w);
            return this;
        }

        /**
         * Set the w value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param w the w value as a Quorum.
         * @return a reference to this object.
         */
        public Builder withW(Quorum w)
        {
            this.w = w;
            return this;
        }

        /**
         * Set the r value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param r the r value as an integer.
         * @return a reference to this object.
         */
        public Builder withR(int r)
        {
            this.r = new Quorum(r);
            return this;
        }

        /**
         * Set the r value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param r the r value as a Quorum.
         * @return a reference to this object.
         */
        public Builder withR(Quorum r)
        {
            this.r = r;
            return this;
        }

        /**
         * Set the pr value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param pr the pr value as an integer.
         * @return a reference to this object.
         */
        public Builder withPr(int pr)
        {
            this.pr = new Quorum(pr);
            return this;
        }

        /**
         * Set the pr value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param pr the pr value as a Quorum.
         * @return a reference to this object.
         */
        public Builder withPr(Quorum pr)
        {
            this.pr = pr;
            return this;
        }

        /**
         * Set the pw value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param pw the pw value as an integer.
         * @return a reference to this object.
         */
        public Builder withPw(int pw)
        {
            this.pw = new Quorum(pw);
            return this;
        }

        /**
         * Set the pw value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param pw the pw value as a Quorum.
         * @return a reference to this object.
         */
        public Builder withPw(Quorum pw)
        {
            this.pw = pw;
            return this;
        }

        /**
         * Set the not_found_ok value. If true a vnode returning notfound for a
         * key increments the r tally. False is higher consistency, true is
         * higher availability.
         *
         * @param ok the not_found_ok value.
         * @return a reference to this object.
         */
        public Builder withNotFoundOk(boolean ok)
        {
            this.notFoundOk = ok;
            return this;
        }

        /**
         * Add a pre-commit hook. The supplied Function must be an Erlang or
         * Named JS function.
         *
         * @param hook the Function to add.
         * @return a reference to this object.
         * @see <a
         * href="http://docs.basho.com/riak/latest/dev/using/commit-hooks/">Using
         * Commit Hooks</a>
         */
        public Builder withPrecommitHook(Function hook)
        {
            if (null == hook || !(!hook.isJavascript() || hook.isNamed()))
            {
                throw new IllegalArgumentException("Must be a named JS or Erlang function.");
            }

            precommitHooks.add(hook);
            return this;
        }

        /**
         * Add a post-commit hook. The supplied Function must be an Erlang or
         * Named JS function.
         *
         * @param hook the Function to add.
         * @return a reference to this object.
         * @see <a
         * href="http://docs.basho.com/riak/latest/dev/using/commit-hooks/">Using
         * Commit Hooks</a>
         */
        public Builder withPostcommitHook(Function hook)
        {
            verifyErlangFunc(hook);
            postcommitHooks.add(hook);
            return this;
        }

        /**
         * Set the old_vclock value.
         *
         * @param oldVClock an long representing a epoch time value.
         * @return a reference to this object.
         * @see <a
         * href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector
         * Clock Pruning</a>
         */
        public Builder withOldVClock(long oldVClock)
        {
            this.oldVClock = oldVClock;
            return this;
        }

        /**
         * Set the young_vclock value.
         *
         * @param youngVClock a long representing a epoch time value.
         * @return a reference to this object.
         * @see <a
         * href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector
         * Clock Pruning</a>
         */
        public Builder withYoungVClock(long youngVClock)
        {
            this.youngVClock = youngVClock;
            return this;
        }

        /**
         * Set the small_vclock value.
         *
         * @param smallVClock a long representing a epoch time value.
         * @return a reference to this object.
         * @see <a
         * href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector
         * Clock Pruning</a>
         */
        public Builder withSmallVClock(long smallVClock)
        {
            this.smallVClock = smallVClock;
            return this;
        }

        /**
         * Set the nVal.
         *
         * @param nVal the number of replicas.
         * @return a reference to this object.
         */
        public Builder withNVal(int nVal)
        {
            if (nVal <= 0)
            {
                throw new IllegalArgumentException("nVal must be >= 1");
            }
            this.nVal = nVal;
            return this;
        }

        /**
         * Enable Legacy Riak Search. Setting this to true causes the search pre-commit
         * hook to be added.
         *
         * <b>Note this is only for legacy Riak (&lt; v2.0) Search support.</b>
         *
         * @param enable add/remove (true/false) the pre-commit hook for Riak
         * Search.
         * @return a reference to this object.
         */
        public Builder withLegacyRiakSearchEnabled(boolean enable)
        {
            search = enable;
            return this;
        }

        /**
         * Associate a Search Index. This only applies if Yokozuna is enabled
         * in Riak.
         *
         * @param indexName The name of the Yokozuna Index to use.
         * @return a reference to this object.
         */
        public Builder withSearchIndex(String indexName)
        {
            if (null == indexName || indexName.length() == 0)
            {
                throw new IllegalArgumentException("Index name cannot be null or zero length");
            }
            this.yokozunaIndex = indexName;
            return this;
        }

        /**
         * Set the HyperLogLog Precision.
         *
         * @param precision the number of bits to use in the HyperLogLog precision.
         *                  Valid values are [4 - 16] inclusive, default is 14 on new buckets.
         *                  <b>NOTE:</b> When changing precision, it may only be reduced from
         *                  it's current value, and never increased.
         * @return a reference to this object.
         */
        public Builder withHllPrecision(int precision)
        {
            if (precision < 4 || precision > 16)
            {
                throw new IllegalArgumentException("Precision must be between 4 and 16, inclusive.");
            }
            this.hllPrecision = precision;
            return this;
        }

        public BucketProperties build()
        {
            return new BucketProperties(this);
        }

        private void verifyErlangFunc(Function f)
        {
            if (null == f || f.isJavascript())
            {
                throw new IllegalArgumentException("Must be an Erlang Function.");
            }
        }
    }
}
