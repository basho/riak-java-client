package com.basho.riak.client.builders;

import java.util.ArrayList;
import java.util.Collection;

import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.bucket.DefaultBucketProperties;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedFunction;

/**
 * Use to create instances of BucketProperties.
 * 
 * @author russell
 * 
 */
public final class BucketPropertiesBuilder {

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
    public int nVal = 3;
    public Boolean lastWriteWins;
    public boolean allowSiblings = false;

    public BucketProperties build() {
        return new DefaultBucketProperties(this);
    }

    /**
     * @param p
     *            the BucketProperties to copy to the builder
     * @return a builder with all values set from p
     */
    public static BucketPropertiesBuilder from(DefaultBucketProperties p) {
        BucketPropertiesBuilder b = new BucketPropertiesBuilder();
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

    public BucketPropertiesBuilder allowSiblings(boolean allowSiblings) {
        this.allowSiblings = allowSiblings;
        return this;
    }

    public BucketPropertiesBuilder lastWriteWins(boolean lastWriteWins) {
        this.lastWriteWins = lastWriteWins;
        return this;
    }

    public BucketPropertiesBuilder nVal(int nVal) {
        this.nVal = nVal;
        return this;
    }

    public BucketPropertiesBuilder backend(String backend) {
        this.backend = backend;
        return this;
    }

    public BucketPropertiesBuilder precommitHooks(Collection<NamedFunction> precommitHooks) {
        this.precommitHooks = new ArrayList<NamedFunction>(precommitHooks);
        return this;
    }

    public BucketPropertiesBuilder addPrecommitHook(NamedFunction preCommitHook) {
        if (this.precommitHooks == null) {
            this.precommitHooks = new ArrayList<NamedFunction>();
        }
        this.precommitHooks.add(preCommitHook);
        return this;
    }

    public BucketPropertiesBuilder postcommitHooks(Collection<NamedErlangFunction> postCommitHooks) {
        this.postcommitHooks = new ArrayList<NamedErlangFunction>(postCommitHooks);
        return this;
    }

    public BucketPropertiesBuilder addPostcommitHook(NamedErlangFunction postcommitHook) {
        if (this.postcommitHooks == null) {
            this.postcommitHooks = new ArrayList<NamedErlangFunction>();
        }
        this.precommitHooks.add(postcommitHook);
        return this;
    }

    public BucketPropertiesBuilder chashKeyFunction(NamedErlangFunction chashKeyFunction) {
        this.chashKeyFunction = chashKeyFunction;
        return this;
    }

    public BucketPropertiesBuilder linkWalkFunction(NamedErlangFunction linkWalkFunction) {
        this.linkWalkFunction = linkWalkFunction;
        return this;
    }

    /**
     * @param smallVClock
     * @return
     */
    public BucketPropertiesBuilder smallVClock(int smallVClock) {
        this.smallVClock = smallVClock;
        return this;
    }

    /**
     * @param bigVClock
     * @return
     */
    public BucketPropertiesBuilder bigVClock(int bigVClock) {
        this.bigVClock = bigVClock;
        return this;
    }

    /**
     * @param youngVClock
     * @return
     */
    public BucketPropertiesBuilder youngVClock(long youngVClock) {
        this.youngVClock = youngVClock;
        return this;
    }

    /**
     * @param oldVClock
     * @return
     */
    public BucketPropertiesBuilder oldVClock(long oldVClock) {
        this.oldVClock = oldVClock;
        return this;
    }

    /**
     * @param r
     * @return
     */
    public BucketPropertiesBuilder r(Quora r) {
        this.r = new Quorum(r);
        return this;
    }

    public BucketPropertiesBuilder r(int r) {
        this.r = new Quorum(r);
        return this;
    }

    /**
     * @param w
     * @return
     */
    public BucketPropertiesBuilder w(Quora w) {
        this.w = new Quorum(w);
        return this;
    }

    public BucketPropertiesBuilder w(int w) {
        this.w = new Quorum(w);
        return this;
    }

    /**
     * @param rw
     * @return
     */
    public BucketPropertiesBuilder rw(Quora rw) {
        this.rw = new Quorum(rw);
        return this;
    }

    public BucketPropertiesBuilder rw(int rw) {
        this.rw = new Quorum(rw);
        return this;
    }

    /**
     * @param dw
     * @return
     */
    public BucketPropertiesBuilder dw(Quora dw) {
        this.dw = new Quorum(dw);
        return this;
    }

    public BucketPropertiesBuilder dw(int dw) {
        this.dw = new Quorum(dw);
        return this;
    }
}