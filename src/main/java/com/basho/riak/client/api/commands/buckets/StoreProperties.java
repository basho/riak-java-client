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

package com.basho.riak.client.api.commands.buckets;

import com.basho.riak.client.api.AsIsRiakCommand;
import com.basho.riak.client.core.operations.StorePropertiesOperation;
import com.basho.riak.client.core.query.functions.Function;

import java.util.Objects;

/**
 * @author Luke Bakken <lbakken@basho.com>
 * @param <S> Namespace or BinaryValue
 * @since 2.2
 */
public abstract class StoreProperties<S> extends AsIsRiakCommand<Void, S>
{
    protected final S bucketOrType;
    protected final Boolean allowMulti;
    protected final String backend;
    protected final Boolean basicQuorum;
    protected final Long bigVClock;
    protected final Function chashkeyFunction;
    protected final Boolean lastWriteWins;
    protected final Function linkWalkFunction;
    protected final Integer rw;
    protected final Integer dw;
    protected final Integer w;
    protected final Integer r;
    protected final Integer pr;
    protected final Integer pw;
    protected final Boolean notFoundOk;
    protected final Function preCommitHook;
    protected final Function postCommitHook;
    protected final Long oldVClock;
    protected final Long youngVClock;
    protected final Long smallVClock;
    protected final Integer nval;
    protected final Boolean legacySearch;
    protected final String searchIndex;
    protected final Integer hllPrecision;

    StoreProperties(S bucketOrType, PropsBuilder builder)
    {
        if (bucketOrType == null)
        {
            throw new IllegalArgumentException("Bucket name or Namespace cannot be null");
        }
        this.bucketOrType = bucketOrType;

        this.allowMulti = builder.allowMulti;
        this.backend = builder.backend;
        this.bigVClock = builder.bigVClock;
        this.chashkeyFunction = builder.chashkeyFunction;
        this.lastWriteWins = builder.lastWriteWins;
        this.basicQuorum = builder.basicQuorum;
        this.linkWalkFunction = builder.linkWalkFunction;
        this.rw = builder.rw;
        this.dw = builder.dw;
        this.w = builder.w;
        this.pr = builder.pr;
        this.pw = builder.pw;
        this.r = builder.r;
        this.notFoundOk = builder.notFoundOk;
        this.preCommitHook = builder.preCommitHook;
        this.postCommitHook = builder.postCommitHook;
        this.oldVClock = builder.oldVClock;
        this.youngVClock = builder.youngVClock;
        this.smallVClock = builder.smallVClock;
        this.nval = builder.nval;
        this.legacySearch = builder.legacySearch;
        this.searchIndex = builder.searchIndex;
        this.hllPrecision = builder.hllPrecision;
    }

    protected void populatePropertiesOperation(StorePropertiesOperation.PropsBuilder builder)
    {
        if (allowMulti != null)
        {
            builder.withAllowMulti(allowMulti);
        }

        if (backend != null)
        {
            builder.withBackend(backend);
        }

        if (basicQuorum != null)
        {
            builder.withBasicQuorum(basicQuorum);
        }

        if (bigVClock != null)
        {
            builder.withBigVClock(bigVClock);
        }

        if (chashkeyFunction != null)
        {
            builder.withChashkeyFunction(chashkeyFunction);
        }

        if (lastWriteWins != null)
        {
            builder.withLastWriteWins(lastWriteWins);
        }

        if (linkWalkFunction != null)
        {
            builder.withLinkwalkFunction(linkWalkFunction);
        }

        if (rw != null)
        {
            builder.withRw(rw);
        }

        if (dw != null)
        {
            builder.withDw(dw);
        }

        if (w != null)
        {
            builder.withW(w);
        }

        if (r != null)
        {
            builder.withR(r);
        }

        if (pr != null)
        {
            builder.withPr(pr);
        }

        if (pw != null)
        {
            builder.withPw(pw);
        }

        if (notFoundOk != null)
        {
            builder.withNotFoundOk(notFoundOk);
        }

        if (preCommitHook != null)
        {
            builder.withPrecommitHook(preCommitHook);
        }

        if (postCommitHook != null)
        {
            builder.withPostcommitHook(postCommitHook);
        }

        if (oldVClock != null)
        {
            builder.withOldVClock(oldVClock);
        }

        if (youngVClock != null)
        {
            builder.withYoungVClock(youngVClock);
        }

        if (smallVClock != null)
        {
            builder.withSmallVClock(smallVClock);
        }

        if (nval != null)
        {
            builder.withNVal(nval);
        }

        if (legacySearch != null)
        {
            builder.withLegacyRiakSearchEnabled(legacySearch);
        }

        if (searchIndex != null)
        {
            builder.withSearchIndex(searchIndex);
        }

        if (hllPrecision != null)
        {
            builder.withHllPrecision(hllPrecision);
        }
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }

        if (!(other instanceof StoreProperties))
        {
            return false;
        }

        StoreProperties otherStoreBucketProperties = (StoreProperties) other;

        return Objects.equals(bucketOrType, otherStoreBucketProperties.bucketOrType) &&
               Objects.equals(allowMulti, otherStoreBucketProperties.allowMulti) &&
               Objects.equals(backend, otherStoreBucketProperties.backend) &&
               Objects.equals(basicQuorum, otherStoreBucketProperties.basicQuorum) &&
               Objects.equals(bigVClock, otherStoreBucketProperties.bigVClock) &&
               Objects.equals(chashkeyFunction, otherStoreBucketProperties.chashkeyFunction) &&
               Objects.equals(lastWriteWins, otherStoreBucketProperties.lastWriteWins) &&
               Objects.equals(linkWalkFunction, otherStoreBucketProperties.linkWalkFunction) &&
               Objects.equals(rw, otherStoreBucketProperties.rw) &&
               Objects.equals(dw, otherStoreBucketProperties.dw) &&
               Objects.equals(w, otherStoreBucketProperties.w) &&
               Objects.equals(r, otherStoreBucketProperties.r) &&
               Objects.equals(pr, otherStoreBucketProperties.pr) &&
               Objects.equals(pw, otherStoreBucketProperties.pw) &&
               Objects.equals(notFoundOk, otherStoreBucketProperties.notFoundOk) &&
               Objects.equals(preCommitHook, otherStoreBucketProperties.preCommitHook) &&
               Objects.equals(postCommitHook, otherStoreBucketProperties.postCommitHook) &&
               Objects.equals(oldVClock, otherStoreBucketProperties.oldVClock) &&
               Objects.equals(youngVClock, otherStoreBucketProperties.youngVClock) &&
               Objects.equals(smallVClock, otherStoreBucketProperties.smallVClock) &&
               Objects.equals(nval, otherStoreBucketProperties.nval) &&
               Objects.equals(legacySearch, otherStoreBucketProperties.legacySearch) &&
               Objects.equals(searchIndex, otherStoreBucketProperties.searchIndex) &&
               Objects.equals(hllPrecision, otherStoreBucketProperties.hllPrecision);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketOrType,
                            allowMulti,
                            backend,
                            basicQuorum,
                            bigVClock,
                            chashkeyFunction,
                            lastWriteWins,
                            linkWalkFunction,
                            rw,
                            dw,
                            w,
                            r,
                            pr,
                            pw,
                            notFoundOk,
                            preCommitHook,
                            postCommitHook,
                            oldVClock,
                            youngVClock,
                            smallVClock,
                            nval,
                            legacySearch,
                            searchIndex,
                            hllPrecision);
    }

    public static abstract class PropsBuilder<T extends PropsBuilder<T>>
    {
        private Boolean allowMulti;
        private String backend;
        private Boolean basicQuorum;
        private Long bigVClock;
        private Function chashkeyFunction;
        private Boolean lastWriteWins;
        private Function linkWalkFunction;
        private Integer rw;
        private Integer dw;
        private Integer w;
        private Integer r;
        private Integer pr;
        private Integer pw;
        private Boolean notFoundOk;
        private Function preCommitHook;
        private Function postCommitHook;
        private Long oldVClock;
        private Long youngVClock;
        private Long smallVClock;
        private Integer nval;
        private Boolean legacySearch;
        private String searchIndex;
        private Integer hllPrecision;

        protected abstract T self();

        /**
         * Set the allow_multi value.
         *
         * @param allow whether to allow sibling objects to be created.
         * @return a reference to this object.
         */
        public T withAllowMulti(boolean allow)
        {
            this.allowMulti = allow;
            return self();
        }

        /**
         * Set the backend used by this bucket or bucket type. Only applies when using
         * {@code riak_kv_multi_backend} in Riak.
         *
         * @param backend the name of the backend to use.
         * @return a reference to this object.
         */
        public T withBackend(String backend)
        {
            if (null == backend || backend.length() == 0)
            {
                throw new IllegalArgumentException("Backend can not be null or zero length");
            }
            this.backend = backend;
            return self();
        }

        /**
         * Set the basic_quorum value.
         * <p>
         * The parameter controls whether a read request should return early in
         * some fail cases. E.g. If a quorum of nodes has already returned
         * notfound/error, don't wait around for the rest.
         *
         * @param use the basic_quorum value.
         * @return a reference to this object.
         */
        public T withBasicQuorum(boolean use)
        {
            this.basicQuorum = use;
            return self();
        }

        /**
         * Set the big_vclock value.
         *
         * @param bigVClock a long representing a epoch time value.
         * @return a reference to this object.
         * @see
         * <a href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector
         * Clock Pruning</a>
         */
        public T withBigVClock(Long bigVClock)
        {
            this.bigVClock = bigVClock;
            return self();
        }

        /**
         * Set the chash_keyfun value.
         *
         * @param func a Function representing the Erlang func to use.
         * @return a reference to this object.
         */
        public T withChashkeyFunction(Function func)
        {
            this.chashkeyFunction = func;
            return self();
        }

        /**
         * Set the last_write_wins value. Unless you really know what you're
         * doing, you probably do not want to set this to true.
         *
         * @param wins whether to ignore vector clocks when writing.
         * @return a reference to this object.
         */
        public T withLastWriteWins(boolean wins)
        {
            this.lastWriteWins = wins;
            return self();
        }

        /**
         * Set the linkfun value.
         *
         * @param func a Function representing the Erlang func to use.
         * @return a reference to this object.
         */
        public T withLinkwalkFunction(Function func)
        {
            this.linkWalkFunction = func;
            return self();
        }

        /**
         * Set the rw value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param rw the rw value as an integer.
         * @return a reference to this object.
         */
        public T withRw(int rw)
        {
            this.rw = rw;
            return self();
        }

        /**
         * Set the dw value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param dw the dw value as an integer.
         * @return a reference to this object.
         */
        public T withDw(int dw)
        {
            this.dw = dw;
            return self();
        }

        /**
         * Set the w value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param w the w value as an integer.
         * @return a reference to this object.
         */
        public T withW(int w)
        {
            this.w = w;
            return self();
        }

        /**
         * Set the r value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param r the r value as an integer.
         * @return a reference to this object.
         */
        public T withR(int r)
        {
            this.r = r;
            return self();
        }

        /**
         * Set the pr value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param pr the pr value as an integer.
         * @return a reference to this object.
         */
        public T withPr(int pr)
        {
            this.pr = pr;
            return self();
        }

        /**
         * Set the pw value. Individual requests (or buckets in a bucket type)
         * can override this.
         *
         * @param pw the pw value as an integer.
         * @return a reference to this object.
         */
        public T withPw(int pw)
        {
            this.pw = pw;
            return self();
        }

        /**
         * Set the not_found_ok value. If true a vnode returning notfound for a
         * key increments the r tally. False is higher consistency, true is
         * higher availability.
         *
         * @param ok the not_found_ok value.
         * @return a reference to this object.
         */
        public T withNotFoundOk(boolean ok)
        {
            this.notFoundOk = ok;
            return self();
        }

        /**
         * Add a pre-commit hook. The supplied Function must be an Erlang or
         * Named JS function.
         *
         * @param hook the Function to add.
         * @return a reference to this object.
         * @see <a href="http://docs.basho.com/riak/latest/dev/using/commit-hooks/">Using Commit Hooks</a>
         */
        public T withPrecommitHook(Function hook)
        {
            if (null == hook || !(!hook.isJavascript() || hook.isNamed()))
            {
                throw new IllegalArgumentException("Must be a named JS or Erlang function.");
            }
            this.preCommitHook = hook;
            return self();
        }

        /**
         * Add a post-commit hook. The supplied Function must be an Erlang or
         * Named JS function.
         *
         * @param hook the Function to add.
         * @return a reference to this object.
         * @see <a href="http://docs.basho.com/riak/latest/dev/using/commit-hooks/">Using Commit Hooks</a>
         */
        public T withPostcommitHook(Function hook)
        {
            this.postCommitHook = hook;
            return self();
        }

        /**
         * Set the old_vclock value.
         *
         * @param oldVClock an long representing a epoch time value.
         * @return a reference to this object.
         * @see
         * <a href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector
         * Clock Pruning</a>
         */
        public T withOldVClock(Long oldVClock)
        {
            this.oldVClock = oldVClock;
            return self();
        }

        /**
         * Set the young_vclock value.
         *
         * @param youngVClock a long representing a epoch time value.
         * @return a reference to this object.
         * @see
         * <a href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector
         * Clock Pruning</a>
         */
        public T withYoungVClock(Long youngVClock)
        {
            this.youngVClock = youngVClock;
            return self();
        }

        /**
         * Set the small_vclock value.
         *
         * @param smallVClock a long representing a epoch time value.
         * @return a reference to this object.
         * @see
         * <a href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector
         * Clock Pruning</a>
         */
        public T withSmallVClock(Long smallVClock)
        {
            this.smallVClock = smallVClock;
            return self();
        }

        /**
         * Set the nVal.
         *
         * @param nVal the number of replicas.
         * @return a reference to this object.
         */
        public T withNVal(int nVal)
        {
            if (nVal <= 0)
            {
                throw new IllegalArgumentException("nVal must be >= 1");
            }
            this.nval = nVal;
            return self();
        }

        /**
         * Enable Legacy Riak Search. Setting this to true causes the search
         * pre-commit hook to be added.
         * <p>
         * <b>Note this is only for legacy Riak (&lt; v2.0) Search support.</b>
         *
         * @param enable add/remove (true/false) the pre-commit hook for Legacy
         *               Riak Search.
         * @return a reference to this object.
         */
        public T withLegacyRiakSearchEnabled(boolean enable)
        {
            this.legacySearch = enable;
            return self();
        }

        /**
         * Associate a Search Index. This only applies if Yokozuna is enabled in
         * Riak v2.0.
         *
         * @param indexName The name of the search index to use.
         * @return a reference to this object.
         */
        public T withSearchIndex(String indexName)
        {
            if (null == indexName || indexName.length() == 0)
            {
                throw new IllegalArgumentException("Index name cannot be null or zero length");
            }
            this.searchIndex = indexName;
            return self();
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
        public T withHllPrecision(int precision)
        {
            if (precision < 4 || precision > 16)
            {
                throw new IllegalArgumentException("Precision must be between 4 and 16, inclusive.");
            }
            this.hllPrecision = precision;
            return self();
        }
    }
}
