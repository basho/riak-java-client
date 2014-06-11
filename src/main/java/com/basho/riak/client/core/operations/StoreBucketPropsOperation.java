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
package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.query.Namespace;
import com.basho.riak.client.query.functions.Function;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import java.util.List;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
//TODO: return some sort of "success" instead of Void
public class StoreBucketPropsOperation extends FutureOperation<Void, Void, Namespace>
{
    private final Namespace namespace;
    private final RiakPB.RpbSetBucketReq.Builder reqBuilder;

    private StoreBucketPropsOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.namespace = builder.namespace;
    }

    @Override
    protected Void convert(List<Void> rawResponse) 
    {
        return null;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakPB.RpbSetBucketReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_SetBucketReq, req.toByteArray());
    }

    @Override
    protected Void decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_SetBucketResp);
        return null;
    }

    @Override
    public Namespace getQueryInfo()
    {
        return namespace;
    }

    static abstract class PropsBuilder<T extends PropsBuilder<T>>
    {

        protected final RiakPB.RpbBucketProps.Builder propsBuilder
                = RiakPB.RpbBucketProps.newBuilder();

        protected abstract T self();

        /**
         * Set the allow_multi value.
         *
         * @param allow whether to allow sibling objects to be created.
         * @return a reference to this object.
         */
        public T withAllowMulti(boolean allow)
        {
            propsBuilder.setAllowMult(allow);
            return self();
        }

        /**
         * Set the backend used by this bucket. Only applies when using
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
            propsBuilder.setBackend(ByteString.copyFromUtf8(backend));
            return self();
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
        public T withBasicQuorum(boolean use)
        {
            propsBuilder.setBasicQuorum(use);
            return self();
        }

        /**
         * Set the big_vclock value.
         *
         * @param bigVClock a long representing a epoch time value.
         * @return a reference to this object.
         * @see <a href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector Clock Pruning</a>
         */
        public T withBigVClock(Long bigVClock)
        {
            propsBuilder.setBigVclock(bigVClock.intValue());
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
            verifyErlangFunc(func);
            propsBuilder.setChashKeyfun(convertModFun(func));
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
            propsBuilder.setLastWriteWins(wins);
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
            verifyErlangFunc(func);
            propsBuilder.setLinkfun(convertModFun(func));
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
            propsBuilder.setRw(rw);
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
            propsBuilder.setDw(dw);
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
            propsBuilder.setW(w);
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
            propsBuilder.setR(r);
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
            propsBuilder.setPr(pr);
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
            propsBuilder.setPw(pw);
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
            propsBuilder.setNotfoundOk(ok);
            return self();
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
        public T withPrecommitHook(Function hook)
        {
            if (null == hook || !(!hook.isJavascript() || hook.isNamed()))
            {
                throw new IllegalArgumentException("Must be a named JS or Erlang function.");
            }

            propsBuilder.addPrecommit(convertHook(hook));
            return self();
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
        public T withPostcommitHook(Function hook)
        {
            verifyErlangFunc(hook);
            propsBuilder.addPostcommit(convertHook(hook));
            return self();
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
        public T withOldVClock(Long oldVClock)
        {
            propsBuilder.setOldVclock(oldVClock.intValue());
            return self();
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
        public T withYoungVClock(Long youngVClock)
        {
            propsBuilder.setYoungVclock(youngVClock.intValue());
            return self();
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
        public T withSmallVClock(Long smallVClock)
        {
            propsBuilder.setSmallVclock(smallVClock.intValue());
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
            propsBuilder.setNVal(nVal);
            return self();
        }

        /**
         * Enable Legacy Riak Search. Setting this to true causes the search
         * pre-commit hook to be added.
         *
         * <b>Note this is only for legacy Riak (&lt; v2.0) Search support.</b>
         *
         * @param enable add/remove (true/false) the pre-commit hook for Legacy
         * Riak Search.
         * @return a reference to this object.
         */
        public T withLegacyRiakSearchEnabled(boolean enable)
        {
            propsBuilder.setSearch(enable);
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
            propsBuilder.setSearchIndex(ByteString.copyFromUtf8(indexName));
            return self();
        }

        private void verifyErlangFunc(Function f)
        {
            if (null == f || f.isJavascript())
            {
                throw new IllegalArgumentException("Must be an Erlang Function.");
            }
        }

        private RiakPB.RpbModFun convertModFun(Function f)
        {
            return RiakPB.RpbModFun.newBuilder()
                    .setModule(ByteString.copyFromUtf8(f.getModule()))
                    .setFunction(ByteString.copyFromUtf8(f.getFunction()))
                    .build();
        }

        private RiakPB.RpbCommitHook convertHook(Function hook)
        {
            RiakPB.RpbCommitHook.Builder builder = RiakPB.RpbCommitHook.newBuilder();
            RiakPB.RpbModFun.Builder mfBuilder = RiakPB.RpbModFun.newBuilder();

            if (hook.isJavascript())
            {
                builder.setName(ByteString.copyFromUtf8(hook.getName()));
            }
            else
            {
                mfBuilder.setModule(ByteString.copyFromUtf8(hook.getModule()));
                mfBuilder.setFunction(ByteString.copyFromUtf8(hook.getFunction()));
                builder.setModfun(mfBuilder);
            }

            return builder.build();
        }
    }

    public static class Builder extends PropsBuilder<Builder>
    {
        private final RiakPB.RpbSetBucketReq.Builder reqBuilder
            = RiakPB.RpbSetBucketReq.newBuilder();
        private final Namespace namespace;
        
        /**
         * Constructs a builder for a StoreBucketPropsOperation.
         * @param namespace The namespace in Riak.
         */
        public Builder(Namespace namespace)
        {
            if (namespace == null)
            {
                throw new IllegalArgumentException("Namespace cannot be null");
            }
            reqBuilder.setBucket(ByteString.copyFrom(namespace.getBucketName().unsafeGetValue()));
            reqBuilder.setType(ByteString.copyFrom(namespace.getBucketType().unsafeGetValue()));
            this.namespace = namespace;
        }
        
        @Override
        protected Builder self()
        {
            return this;
        }
        
        public StoreBucketPropsOperation build()
        {
            reqBuilder.setProps(propsBuilder);
            return new StoreBucketPropsOperation(this);
        }
    }
}
