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

import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.query.functions.Function;
import java.util.LinkedList;
import java.util.List;

/**
 * Bucket properties used for buckets and bucket types.
 * @author Brian Roach <roach at basho dot com>
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
    private Integer bigVClock;
    private Integer smallVClock;
    private String backend;
    private Integer nVal;
    private Boolean lastWriteWins;
    private Boolean allowSiblings;
    private String yokozunaIndex;
    
    public BucketProperties()
    {
    }
    
    /**
     * Set the linkwalk_fun.
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
     * The linkwalk_fun.
     * 
     * @return the link walking function for the bucket, or null.
     */
    public Function getLinkwalkFunction()
    {
        return linkwalkFunction;
    }
    
    /**
     * Set the chash_keyfun.
     * @param func a Function representing the Erlang func to use.
     * @return 
     */
    public BucketProperties withChashkeyFunction(Function func)
    {
        verifyErlangFunc(func);
        this.chashKeyFunction = func;
        return this;
    }
    
    /**
     * The chash_keyfun for this bucket.
     * 
     * @return the key hashing function for the bucket, or null.
     */
    public Function getChashKeyFunction()
    {
        return chashKeyFunction;
    }
    
    public BucketProperties withRw(int rw)
    {
        this.rw = new Quorum(rw);
        return this;
    }
    
    public BucketProperties withRw(Quorum rw)
    {
        this.rw = rw;
        return this;
    }
    
    public BucketProperties withRw(Quora rw)
    {
        this.rw = new Quorum(rw);
        return this;
    }
    
    public Quorum getRw()
    {
        return rw;
    }
    
    public BucketProperties withDw(int dw)
    {
        this.dw = new Quorum(dw);
        return this;
    }
    
    public BucketProperties withDw(Quorum dw)
    {
        this.dw = dw;
        return this;
    }
    
    public BucketProperties withDw(Quora dw)
    {
        this.dw = new Quorum(dw);
        return this;
    }
    
    public Quorum getDw()
    {
        return dw;
    }
    
    public BucketProperties withW(int w)
    {
        this.w = new Quorum(w);
        return this;
    }
    
    public BucketProperties withW(Quorum w)
    {
        this.w = w;
        return this;
    }
    
    public BucketProperties withW(Quora w)
    {
        this.w = new Quorum(w);
        return this;
    }
    
    public Quorum getW()
    {
        return w;
    }
    
    public BucketProperties withR(int r)
    {
        this.r = new Quorum(r);
        return this;
    }
    
    public BucketProperties withR(Quorum r)
    {
        this.r = r;
        return this;
    }
    
    public BucketProperties withR(Quora r)
    {
        this.r = new Quorum(r);
        return this;
    }
    
    public Quorum getR()
    {
        return this.r;
    }
    
    public BucketProperties withPr(int pr)
    {
        this.pr = new Quorum(pr);
        return this;
    }
    
    public BucketProperties withPr(Quorum pr)
    {
        this.pr = pr;
        return this;
    }
    
    public BucketProperties withPr(Quora pr)
    {
        this.pr = new Quorum(pr);
        return this;
    }
    
    public Quorum getPr()
    {
        return pr;
    }
    
    public BucketProperties withPw(int pw)
    {
        this.pw = new Quorum(pw);
        return this;
    }
    
    public BucketProperties withPw(Quorum pw)
    {
        this.pw = pw;
        return this;
    }
    
    public BucketProperties withPw(Quora pw)
    {
        this.pw = new Quorum(pw);
        return this;
    }
    
    public Quorum getPw()
    {
        return pw;
    }
    
    public BucketProperties notFoundOk(boolean ok)
    {
        this.notFoundOk = ok;
        return this;
    }
    
    public boolean getNotFoundOk()
    {
        return notFoundOk;
    }
    
    public BucketProperties useBasicQuorum(boolean use)
    {
        this.basicQuorum = use;
        return this;
    }
    
    public boolean getBasicQuorum()
    {
        return basicQuorum;
    }
    
    public BucketProperties withPrecommitHook(Function hook)
    {
        if (null == hook || !(!hook.isJavascript() || hook.isNamed()))
        {
            throw new IllegalArgumentException("Must be a named JS or Erlang function.");
        }
        
        precommitHooks.add(hook);
        return this;
    }
    
    public List<Function> getPrecommitHooks()
    {
        return precommitHooks;
    }
    
    public BucketProperties withPostcommitHook(Function hook)
    {
        verifyErlangFunc(hook);
        postcommitHooks.add(hook);
        return this;
    }
    
    public List<Function> getPostcommitHooks()
    {
        return postcommitHooks;
    }
    
    public BucketProperties withOldVClock(long oldVClock)
    {
        this.oldVClock = oldVClock;
        return this;
    }
    
    public long getOldVClock()
    {
        return oldVClock;
    }
    
    public BucketProperties withYoungVClock(long youngVClock)
    {
        this.youngVClock = youngVClock;
        return this;
    }
    
    public long getYoungVClock()
    {
        return youngVClock;
    }
    
    public BucketProperties withBigVClock(int bigVClock)
    {
        this.bigVClock = bigVClock;
        return this;
    }
    
    public int getBigVClock()
    {
        return bigVClock;
    }
    
    public BucketProperties withSmallVClock(int smallVClock)
    {
        this.smallVClock = smallVClock;
        return this;
    }
    
    public int getSmallVClock()
    {
        return smallVClock;
    }
    
    public BucketProperties usingBackend(String backend)
    {
        if (null == backend || backend.length() == 0)
        {
            throw new IllegalArgumentException("Backend can not be null or zero length");
        }
        this.backend = backend;
        return this;
    }
    
    public String getBackend()
    {
        return backend;
    }
    
    public BucketProperties withNVal(int nVal)
    {
        if (nVal <= 0)
        {
            throw new IllegalArgumentException("nVal must be >= 1");
        }
        this.nVal = nVal;
        return this;
    }
    
    public int getNVal()
    {
        return nVal;
    }
    
    public BucketProperties lastWriteWins(boolean wins)
    {
        this.lastWriteWins = wins;
        return this;
    }
    
    public boolean lastWriteWins()
    {
        return lastWriteWins;
    }
    
    public BucketProperties allowSiblings(boolean allow)
    {
        this.allowSiblings = allow;
        return this;
    }
    
    public boolean siblingsAllowed()
    {
        return allowSiblings;
    }
    
    public BucketProperties enableRiakSearch(boolean enable)
    {
        if (!precommitHooks.contains(Function.SEARCH_PRECOMMIT_HOOK))
        {
            precommitHooks.add(Function.SEARCH_PRECOMMIT_HOOK);
        }
        return this;
    }
    
    public boolean riakSearchEnabled()
    {
        if (precommitHooks.contains(Function.SEARCH_PRECOMMIT_HOOK)) 
        {
            return true;
        }
        return false;
    }
    
    public BucketProperties withYokozunaIndex(String indexName)
    {
        if (null == indexName || indexName.length() == 0)
        {
            throw new IllegalArgumentException("Index name cannot be null or zero length");
        }
        this.yokozunaIndex = indexName;
        return this;
    }
    
    public boolean hasYokozunaIndex()
    {
        return yokozunaIndex != null;
    }
    
    public String getYokozunaIndex()
    {
        return yokozunaIndex;
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
            (notFoundOk == other.notFoundOk || (notFoundOk != null && notFoundOk.equals(other.notFoundOk))) &&
            (basicQuorum == other.basicQuorum || (basicQuorum != null && basicQuorum.equals(other.basicQuorum))) &&
            (precommitHooks.equals(other.precommitHooks)) &&
            (postcommitHooks.equals(other.postcommitHooks)) &&
            (oldVClock == other.oldVClock || (oldVClock != null && oldVClock.equals(other.oldVClock))) &&
            (youngVClock == other.youngVClock || (youngVClock != null && youngVClock.equals(other.youngVClock))) &&
            (bigVClock == other.bigVClock || (bigVClock != null && bigVClock.equals(other.bigVClock))) &&
            (smallVClock == other.smallVClock || (smallVClock != null && smallVClock.equals(other.smallVClock))) &&
            (backend == other.backend || (backend != null && backend.equals(other.backend))) &&
            (nVal == other.nVal || (nVal != null && nVal.equals(other.nVal))) &&
            (lastWriteWins == other.lastWriteWins || (lastWriteWins != null && lastWriteWins.equals(other.lastWriteWins))) &&
            (allowSiblings == other.allowSiblings || (allowSiblings != null && allowSiblings.equals(other.allowSiblings))) &&
            (yokozunaIndex == other.yokozunaIndex || (yokozunaIndex != null && yokozunaIndex.equals(other.yokozunaIndex)))
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
