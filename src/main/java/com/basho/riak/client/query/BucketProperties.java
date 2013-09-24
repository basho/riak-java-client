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
package com.basho.riak.client.query;

import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.query.functions.Function;
import java.util.LinkedList;
import java.util.List;

/**
 * Bucket properties used for buckets and bucket types.
 * 
 * Note that when instantiating a new instance there are no default values. Calling
 * a getter for a specific property will return null if it has not been explicitly 
 * set. Unset properties are ignored when writing to a bucket or bucket type in Riak.
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class BucketProperties
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
    private List<Function> precommitHooks =
        new LinkedList<Function>();
    private List<Function> postcommitHooks =
        new LinkedList<Function>();
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
    
    public BucketProperties()
    {
    }
    
    /**
     * Set the linkfun value.
     * @param func a Function representing the Erlang func to use.
     * @return a reference to this object.
     */
    public BucketProperties withLinkwalkFunction(Function func)
    {
        verifyErlangFunc(func);
        this.linkwalkFunction = func;
        return this;
    }
    
    /**
     * Determine if a linkfun value has been set.
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
     * Set the chash_keyfun value.
     * @param func a Function representing the Erlang func to use.
     * @return a reference to this object.
     */
    public BucketProperties withChashkeyFunction(Function func)
    {
        verifyErlangFunc(func);
        this.chashKeyFunction = func;
        return this;
    }
    
    /**
     * Determine if a chash_keyfun value has been set.
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
     * Set the rw value.
     * Individual requests (or buckets in a bucket type) can override this.
     * @param rw the rw value as an integer.
     * @return a reference to this object.
     */
    public BucketProperties withRw(int rw)
    {
        this.rw = new Quorum(rw);
        return this;
    }
    
    /**
     * Set the rw value.
     * Individual requests (or buckets in a bucket type) can override this.
     * @param rw the rw value as a Quorum.
     * @return a reference to this object.
     */
    public BucketProperties withRw(Quorum rw)
    {
        this.rw = rw;
        return this;
    }
    
    /**
     * Determine if an rw value has been set.
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
     * Set the dw value.
     * Individual requests (or buckets in a bucket type) can override this.
     * @param dw the dw value as an integer.
     * @return a reference to this object.
     */
    public BucketProperties withDw(int dw)
    {
        this.dw = new Quorum(dw);
        return this;
    }
    
    /**
     * Set the dw value.
     * Individual requests (or buckets in a bucket type) can override this.
     * @param dw the dw value as a Quorum.
     * @return a reference to this object.
     */
    public BucketProperties withDw(Quorum dw)
    {
        this.dw = dw;
        return this;
    }
    
    /**
     * Determine if a dw value has been set.
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
     * Set the w value.
     * Individual requests (or buckets in a bucket type) can override this.
     * @param w the w value as an integer.
     * @return a reference to this object.
     */
    public BucketProperties withW(int w)
    {
        this.w = new Quorum(w);
        return this;
    }
    
    /**
     * Set the w value.
     * Individual requests (or buckets in a bucket type) can override this.
     * @param w the w value as a Quorum.
     * @return a reference to this object.
     */
    public BucketProperties withW(Quorum w)
    {
        this.w = w;
        return this;
    }
    
    /**
     * Determine if a w value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasW()
    {
        return w != null;
    }
    /**
     * Get the w value.
     * @return the w value as a Quorum, or null if not set.
     */
    public Quorum getW()
    {
        return w;
    }
    
    /**
     * Set the r value.
     * Individual requests (or buckets in a bucket type) can override this.
     * @param r the r value as an integer.
     * @return a reference to this object.
     */
    public BucketProperties withR(int r)
    {
        this.r = new Quorum(r);
        return this;
    }
    
    /**
     * Set the r value.
     * Individual requests (or buckets in a bucket type) can override this.
     * @param r the r value as a Quorum.
     * @return a reference to this object.
     */
    public BucketProperties withR(Quorum r)
    {
        this.r = r;
        return this;
    }
    
    /**
     * Determine if an r value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasR()
    {
        return r != null;
    }
    
    /**
     * Get the r value.
     * @return the r value as a Quorum, or null if not set.
     */
    public Quorum getR()
    {
        return this.r;
    }
    
    /**
     * Set the pr value.
     * Individual requests (or buckets in a bucket type) can override this.
     * @param pr the pr value as an integer.
     * @return a reference to this object.
     */
    public BucketProperties withPr(int pr)
    {
        this.pr = new Quorum(pr);
        return this;
    }
    
    /**
     * Set the pr value.
     * Individual requests (or buckets in a bucket type) can override this.
     * @param pr the pr value as a Quorum.
     * @return a reference to this object.
     */
    public BucketProperties withPr(Quorum pr)
    {
        this.pr = pr;
        return this;
    }
    
    /**
     * Determine if a pr value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasPr()
    {
        return pr != null;
    }
    
    /**
     * Get the pr value.
     * @return the pr value as a Quorum, or null.
     */
    public Quorum getPr()
    {
        return pr;
    }
    
    /**
     * Set the pw value.
     * Individual requests (or buckets in a bucket type) can override this.
     * @param pw the pw value as an integer.
     * @return a reference to this object.
     */
    public BucketProperties withPw(int pw)
    {
        this.pw = new Quorum(pw);
        return this;
    }
    
    /**
     * Set the pw value.
     * Individual requests (or buckets in a bucket type) can override this.
     * @param pw the pw value as a Quorum.
     * @return a reference to this object.
     */
    public BucketProperties withPw(Quorum pw)
    {
        this.pw = pw;
        return this;
    }
    
    /**
     * Determine if a pw value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasPw()
    {
        return pw != null;
    }
    /**
     * Set the pw value.
     * @return the pw value as a Quorum, or null.
     */
    public Quorum getPw()
    {
        return pw;
    }
    
    /**
     * Set the not_found_ok value.
     * If true a vnode returning notfound for a key increments the r tally.
     * False is higher consistency, true is higher availability.
     * 
     * @param ok the not_found_ok value.
     * @return a reference to this object.
     */
    public BucketProperties withNotFoundOk(boolean ok)
    {
        this.notFoundOk = ok;
        return this;
    }
    
    /**
     * Determine if a not_found_ok value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasNotFoundOk()
    {
        return notFoundOk != null;
    }
    
    /**
     * Get the not_found_ok value.
     * @return the not_found_ok value or null if not set.
     * @see BucketProperties#withNotFoundOk(boolean)
     */
    public Boolean getNotFoundOk()
    {
        return notFoundOk;
    }
    
    /**
     * Set the basic_quorum value.
     * 
     * The parameter controls whether a read request should return early in
     * some fail cases. 
     * E.g. If a quorum of nodes has already
     * returned notfound/error, don't wait around for the rest.
     * 
     * @param use the basic_quorum value.
     * @return a reference to this object.
     */
    public BucketProperties withBasicQuorum(boolean use)
    {
        this.basicQuorum = use;
        return this;
    }
    
    /**
     * Determine if a basic_quorum value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasBasicQuorum()
    {
        return basicQuorum != null;
    }
    
    /**
     * Get the basic_quorum value.
     * @return the basic_quorum value, or null if not set.
     * @see BucketProperties#withBasicQuorum(boolean) 
     */
    public Boolean getBasicQuorum()
    {
        return basicQuorum;
    }
    
    /**
     * Add a pre-commit hook. 
     * The supplied Function must be an Erlang or Named JS function.
     * @param hook the Function to add. 
     * @return a reference to this object.
     * @see <a href="http://docs.basho.com/riak/latest/dev/using/commit-hooks/">Using Commit Hooks</a>
     */
    public BucketProperties withPrecommitHook(Function hook)
    {
        if (null == hook || !(!hook.isJavascript() || hook.isNamed()))
        {
            throw new IllegalArgumentException("Must be a named JS or Erlang function.");
        }
        
        precommitHooks.add(hook);
        return this;
    }
    
    /**
     * Determine if any pre-commit hooks have been added.
     * @return true if set, false otherwise.
     */
    public boolean hasPrecommitHooks()
    {
        return !precommitHooks.isEmpty();
    }
    
    /**
     * Get the list of pre-commit hooks.
     * @return a List containing the pre-commit hooks.
     * @see BucketProperties#withPrecommitHook(com.basho.riak.client.query.functions.Function) 
     */
    public List<Function> getPrecommitHooks()
    {
        return precommitHooks;
    }
    
    /**
     * Add a post-commit hook. 
     * The supplied Function must be an Erlang or Named JS function.
     * @param hook the Function to add. 
     * @return a reference to this object.
     * @see <a href="http://docs.basho.com/riak/latest/dev/using/commit-hooks/">Using Commit Hooks</a>
     */
    public BucketProperties withPostcommitHook(Function hook)
    {
        verifyErlangFunc(hook);
        postcommitHooks.add(hook);
        return this;
    }
    
    /**
     * Determine if any post-commit hooks have been added.
     * @return true if set, false otherwise.
     */
    public boolean hasPostcommitHooks()
    {
        return !postcommitHooks.isEmpty();
    }
    
    /**
     * Get the list of post-commit hooks.
     * @return a List containing the post-commit hooks
     * @see BucketProperties#withPostcommitHook(com.basho.riak.client.query.functions.Function) 
     */
    public List<Function> getPostcommitHooks()
    {
        return postcommitHooks;
    }
    
    /**
     * Set the old_vclock value.
     * 
     * @param oldVClock an long representing a epoch time value.
     * @return a reference to this object.
     * @see <a href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector Clock Pruning</a> for details.
     */
    public BucketProperties withOldVClock(long oldVClock)
    {
        this.oldVClock = oldVClock;
        return this;
    }
    
    /**
     * Determine if an old_vclock value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasOldVClock()
    {
        return oldVClock != null;
    }
    
    /**
     * Get the old_vclock value.
     * @return the old_vclock value as a long or null if not set.
     * @see BucketProperties#withOldVClock(long) 
     */
    public Long getOldVClock()
    {
        return oldVClock;
    }
    
    /**
     * Set the young_vclock value.
     * 
     * @param youngVClock a long representing a epoch time value.
     * @return a reference to this object.
     * @see <a href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector Clock Pruning</a> for details.
     */
    public BucketProperties withYoungVClock(long youngVClock)
    {
        this.youngVClock = youngVClock;
        return this;
    }
    
    /**
     * Determine if an young_vclock value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasYoungVClock()
    {
        return youngVClock != null;
    }
    
    /**
     * Get the young_vclock value.
     * @return the young_vclock value as an long or null if not set.
     * @see BucketProperties#withYoungVClock(long) 
     */
    public Long getYoungVClock()
    {
        return youngVClock;
    }
    
    /**
     * Set the big_vclock value.
     * 
     * @param bigVClock a long representing a epoch time value.
     * @return a reference to this object.
     * @see <a href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector Clock Pruning</a> for details.
     */
    public BucketProperties withBigVClock(long bigVClock)
    {
        this.bigVClock = bigVClock;
        return this;
    }
    
    /**
     * Determine if an big_vclock value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasBigVClock()
    {
        return bigVClock != null;
    }
    
    /**
     * Get the big_vclock value.
     * @return the big_vclock value as a long or null if not set.
     * @see BucketProperties#withBigVClock(long) 
     */
    public Long getBigVClock()
    {
        return bigVClock;
    }
    
    /**
     * Set the small_vclock value.
     * 
     * @param smallVClock a long representing a epoch time value.
     * @return a reference to this object.
     * @see <a href="http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Vector-Clock-Pruning">Vector Clock Pruning</a> for details.
     */
    public BucketProperties withSmallVClock(long smallVClock)
    {
        this.smallVClock = smallVClock;
        return this;
    }
    
    /**
     * Determine if an small_vclock value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasSmallVClock()
    {
        return smallVClock != null;
    }
    
    /**
     * Get the small_vclock value.
     * @return the small_vclock value as a long or null if not set.
     * @see BucketProperties#withSmallVClock(long) 
     */
    public Long getSmallVClock()
    {
        return smallVClock;
    }
    
    /**
     * Set the backend used by this bucket.
     * Only applies when using {@code riak_kv_multi_backend} in Riak.
     * @param backend the name of the backend to use. 
     * @return a reference to this object.
     */
    public BucketProperties withBackend(String backend)
    {
        if (null == backend || backend.length() == 0)
        {
            throw new IllegalArgumentException("Backend can not be null or zero length");
        }
        this.backend = backend;
        return this;
    }
    
    /**
     * Determine if an backend value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasBackend()
    {
        return backend != null;
    }
    
    /**
     * Get the backend.
     * Only applies when using {@code riak_kv_multi_backend} in Riak.
     * @return the name of the backend or null if not set.
     */
    public String getBackend()
    {
        return backend;
    }
    
    /**
     * Set the nVal.
     * @param nVal the number of replicas.
     * @return a reference to this object.
     */
    public BucketProperties withNVal(int nVal)
    {
        if (nVal <= 0)
        {
            throw new IllegalArgumentException("nVal must be >= 1");
        }
        this.nVal = nVal;
        return this;
    }
    
    /**
     * Determine if an nVal value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasNVal()
    {
        return nVal != null;
    }
    
    /**
     * Get the nVal.
     * @return the nVal value or null if not set.
     */
    public Integer getNVal()
    {
        return nVal;
    }
    
    /**
     * Set the last_write_wins value.
     * Unless you really know what you're doing, you probably do not want to set 
     * this to true.
     * @param wins whether to ignore vector clocks when writing.
     * @return a reference to this object.
     */
    public BucketProperties withLastWriteWins(boolean wins)
    {
        this.lastWriteWins = wins;
        return this;
    }
    
    /**
     * Determine if a last_write_wins value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasLastWriteWins()
    {
        return lastWriteWins != null;
    }
    
    /**
     * Get the last_write_wins value.
     * @return the last_write_wins value or null if not set.
     */
    public Boolean getLastWriteWins()
    {
        return lastWriteWins;
    }
    
    /**
     * Set the allow_multi value.
     * 
     * @param allow whether to allow sibling objects to be created.
     * @return a reference to this object.
     */
    public BucketProperties withAllowMulti(boolean allow)
    {
        this.allowSiblings = allow;
        return this;
    }
    
    /**
     * Determine if an allow_multi value has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasAllowMulti()
    {
        return allowSiblings != null;
    }
    
    /**
     * Get the allow_multi value.
     * @return the allow_multi value or null if not set.
     */
    public Boolean getAllowMulti()
    {
        return allowSiblings;
    }
    
    /**
     * Enable Riak Search.
     * Setting this to true causes the search pre-commit hook to be added. 
     * @param enable add/remove (true/false) the pre-commit hook for Riak Search.
     * @return a reference to this object.
     */
    public BucketProperties withRiakSearchEnabled(boolean enable)
    {
        search = Boolean.TRUE;
        return this;
    }
    
    /**
     * Determine if Riak Search has been enabled.
     * @return true if set, false otherwise.
     */
    public boolean hasRiakSearchEnabled()
    {
        return search != null;
    }
    
    /**
     * Determine if Riak Search is enabled.
     * @return true if Riak Search is enabled, false otherwise.
     */
    public Boolean getRiakSearchEnabled()
    {
        return search;
    }
    
    /**
     * Associate a Yokozuna Index.
     * This only applies if Yokozuna is enabled in Riak. 
     * @param indexName The name of the Yokozuna Index to use.
     * @return a reference to this object.
     */
    public BucketProperties withYokozunaIndex(String indexName)
    {
        if (null == indexName || indexName.length() == 0)
        {
            throw new IllegalArgumentException("Index name cannot be null or zero length");
        }
        this.yokozunaIndex = indexName;
        return this;
    }
    
    /**
     * Determine if a Yokozuna Index has been set.
     * @return true if set, false otherwise.
     */
    public boolean hasYokozunaIndex()
    {
        return yokozunaIndex != null;
    }
    
    /**
     * Get the associated Yokozuna Index.
     * @return the Yokozuna index name or null if not set.
     */
    public String getYokozunaIndex()
    {
        return yokozunaIndex;
    }
    
    @Override 
    public String toString() 
    {
        return String.format("DefaultBucketProperties [allowSiblings=%s, "
            + "lastWriteWins=%s, nVal=%s, backend=%s, vclockProps=%s, "
            + "precommitHooks=%s, postcommitHooks=%s, "
            + "capProps=[rw=%s,dw=%s,w=%s,r=%s,pr=%s,pw=%s]"
            + ", chashKeyFunction=%s, linkWalkFunction=%s, search=%s,"
            + "yokozunaIndex=%s]",
            allowSiblings, lastWriteWins, nVal, backend, rw, dw, 
            w, r, pr, pw, precommitHooks, postcommitHooks,
            chashKeyFunction, linkwalkFunction, search,
            yokozunaIndex);
    }
    
    private void verifyErlangFunc(Function f)
    {
        if (null == f || f.isJavascript())
        {
            throw new IllegalArgumentException("Must be an Erlang Function.");
        }
    }
    
    @Override 
    public int hashCode() 
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((null == linkwalkFunction) ? 0 : linkwalkFunction.hashCode());
        result = prime * result + ((null == chashKeyFunction) ? 0 : chashKeyFunction.hashCode());
        result = prime * result + ((null == rw) ? 0 : rw.hashCode());
        result = prime * result + ((null == dw) ? 0 : dw.hashCode());
        result = prime * result + ((null == w) ? 0 : w.hashCode());
        result = prime * result + ((null == r) ? 0 : r.hashCode());
        result = prime * result + ((null == pr) ? 0 : pr.hashCode());
        result = prime * result + ((null == pw) ? 0 : pw.hashCode());
        result = prime * result + ((null == notFoundOk) ? 0 : notFoundOk.hashCode());
        result = prime * result + ((null == basicQuorum) ? 0 : basicQuorum.hashCode());
        result = prime * result + precommitHooks.hashCode();
        result = prime * result + postcommitHooks.hashCode();
        result = prime * result + (null == oldVClock ? 0 : oldVClock.hashCode());
        result = prime * result + (null == youngVClock ? 0 : youngVClock.hashCode());
        result = prime * result + (null == bigVClock ? 0 : bigVClock.hashCode());
        result = prime * result + (null == smallVClock ? 0 : smallVClock.hashCode());
        result = prime * result + (null == backend ? 0 : backend.hashCode());
        result = prime * result + (null == nVal ? 0 : nVal.hashCode());
        result = prime * result + (null == lastWriteWins ? 0 : lastWriteWins.hashCode());
        result = prime * result + (null == allowSiblings ? 0 : allowSiblings.hashCode());
        result = prime * result + (null == yokozunaIndex ? 0 : yokozunaIndex.hashCode());
        result = prime * result + (null == search ? 0 : search.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (null == obj)
        {
            return false;
        }
        if (!(obj instanceof BucketProperties))
        {
            return false;
        }
        
        BucketProperties other = (BucketProperties)obj;
        if ((linkwalkFunction == other.linkwalkFunction || 
              (linkwalkFunction != null && linkwalkFunction.equals(other.linkwalkFunction))) &&
            (chashKeyFunction == other.chashKeyFunction || 
              (chashKeyFunction != null && chashKeyFunction.equals(other.chashKeyFunction))) &&
            (rw == other.rw || (rw != null && rw.equals(other.rw))) &&
            (dw == other.dw || (dw != null && dw.equals(other.rw))) &&
            (w == other.w || (w != null && w.equals(other.w))) &&
            (r == other.r || (r != null && r.equals(other.r))) &&
            (pr == other.pr || (pr != null && pr.equals(other.pr))) &&
            (pw == other.pw || (pw != null && pw.equals(other.pw))) &&
            (notFoundOk == other.notFoundOk || 
              (notFoundOk != null && notFoundOk.equals(other.notFoundOk))) &&
            (basicQuorum == other.basicQuorum || 
              (basicQuorum != null && basicQuorum.equals(other.basicQuorum))) &&
            (precommitHooks.equals(other.precommitHooks)) &&
            (postcommitHooks.equals(other.postcommitHooks)) &&
            (oldVClock == other.oldVClock || 
              (oldVClock != null && oldVClock.equals(other.oldVClock))) &&
            (youngVClock == other.youngVClock || 
              (youngVClock != null && youngVClock.equals(other.youngVClock))) &&
            (bigVClock == other.bigVClock || 
              (bigVClock != null && bigVClock.equals(other.bigVClock))) &&
            (smallVClock == other.smallVClock || 
              (smallVClock != null && smallVClock.equals(other.smallVClock))) &&
            (backend == other.backend || 
              (backend != null && backend.equals(other.backend))) &&
            (nVal == other.nVal || (nVal != null && nVal.equals(other.nVal))) &&
            (lastWriteWins == other.lastWriteWins || 
              (lastWriteWins != null && lastWriteWins.equals(other.lastWriteWins))) &&
            (allowSiblings == other.allowSiblings || 
              (allowSiblings != null && allowSiblings.equals(other.allowSiblings))) &&
            (yokozunaIndex == other.yokozunaIndex || 
              (yokozunaIndex != null && yokozunaIndex.equals(other.yokozunaIndex))) &&
            (search == other.search || (search != null && search.equals(other.search)))
            
           )
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    
}
