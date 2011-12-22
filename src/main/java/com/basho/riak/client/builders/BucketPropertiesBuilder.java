package com.basho.riak.client.builders;

import java.util.ArrayList;
import java.util.Collection;

import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.bucket.DefaultBucketProperties;
import com.basho.riak.client.bucket.TunableCAPProps;
import com.basho.riak.client.bucket.VClockPruneProps;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedFunction;

/**
 * Used to create instances of {@link BucketProperties}.
 * 
 * <p>
 * All parameters are optional, only nVal has a default value (3)
 * </p>
 * 
 * @author russell
 * 
 */
public final class BucketPropertiesBuilder {

    private NamedErlangFunction linkWalkFunction;
    private NamedErlangFunction chashKeyFunction;
    private Quorum rw;
    private Quorum dw;
    private Quorum w;
    private Quorum r;
    private Quorum pr;
    private Quorum pw;
    private Boolean notFoundOK;
    private boolean basicQuorum;
    private Collection<NamedFunction> precommitHooks;
    private Collection<NamedErlangFunction> postcommitHooks;
    private Long oldVClock;
    private Long youngVClock;
    private Integer bigVClock;
    private Integer smallVClock;
    private String backend;
    private int nVal = 3;
    private Boolean lastWriteWins;
    private boolean allowSiblings = false;
    private Boolean search;

    public BucketProperties build() {
        return new DefaultBucketProperties(allowSiblings, lastWriteWins, nVal, backend,
                                           new VClockPruneProps(smallVClock, bigVClock, youngVClock, oldVClock),
                                           precommitHooks, postcommitHooks,
                                           new TunableCAPProps(r, w, dw, rw, pr, pw, basicQuorum, notFoundOK),
                                           chashKeyFunction, linkWalkFunction,
                                           search);
    }

    /**
     * @param p
     *            the BucketProperties to copy to the builder
     * @return a builder with all values set from p
     */
    public static BucketPropertiesBuilder from(BucketProperties p) {
        BucketPropertiesBuilder b = new BucketPropertiesBuilder();
        b.allowSiblings = p.getAllowSiblings();
        b.lastWriteWins = p.getLastWriteWins();
        b.nVal = p.getNVal();
        b.backend = p.getBackend();
        b.smallVClock = p.getSmallVClock();
        b.bigVClock = p.getBigVClock();
        b.youngVClock = p.getYoungVClock();
        b.oldVClock = p.getOldVClock();
        b.precommitHooks = p.getPrecommitHooks() == null ? null : new ArrayList<NamedFunction>(p.getPrecommitHooks());
        b.postcommitHooks = p.getPostcommitHooks() == null ? null : new ArrayList<NamedErlangFunction>(p.getPostcommitHooks());
        b.r = p.getR();
        b.w = p.getW();
        b.dw = p.getDW();
        b.rw = p.getRW();
        b.pr = p.getPR();
        b.pw = p.getPW();
        b.basicQuorum = p.getBasicQuorum();
        b.notFoundOK = p.getNotFoundOK();
        b.search = p.isSearchEnabled();
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
        if(precommitHooks != null) {
            this.precommitHooks = new ArrayList<NamedFunction>(precommitHooks);
        }
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
        if(postCommitHooks != null) {
            this.postcommitHooks = new ArrayList<NamedErlangFunction>(postCommitHooks);
        }
        return this;
    }

    public BucketPropertiesBuilder addPostcommitHook(NamedErlangFunction postcommitHook) {
        if (this.postcommitHooks == null) {
            this.postcommitHooks = new ArrayList<NamedErlangFunction>();
        }
        this.postcommitHooks.add(postcommitHook);
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

    public BucketPropertiesBuilder r(Quorum r) {
        this.r = r;
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

    public BucketPropertiesBuilder w(Quorum w) {
        this.w = w;
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

    public BucketPropertiesBuilder rw(Quorum rw) {
        this.rw = rw;
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

    public BucketPropertiesBuilder dw(Quorum dw) {
        this.dw = dw;
        return this;
    }

    /**
     * @param pr
     * @return
     */
    public BucketPropertiesBuilder pr(Quora pr) {
        this.pr = new Quorum(pr);
        return this;
    }

    public BucketPropertiesBuilder pr(int pr) {
        this.pr = new Quorum(pr);
        return this;
    }

    public BucketPropertiesBuilder pr(Quorum pr) {
        this.pr = pr;
        return this;
    }

    /**
     * @param pw
     * @return
     */
    public BucketPropertiesBuilder pw(Quora pw) {
        this.pw = new Quorum(pw);
        return this;
    }

    public BucketPropertiesBuilder pw(int pw) {
        this.pw = new Quorum(pw);
        return this;
    }

    public BucketPropertiesBuilder pw(Quorum pw) {
        this.pw = pw;
        return this;
    }

    /**
     * Set default basicQuorum value for bucket
     * 
     * @param basicQuorum
     * @return this
     */
    public BucketPropertiesBuilder basicQuorum(boolean basicQuorum) {
        this.basicQuorum = basicQuorum;
        return this;
    }

    /**
     * Set default notfound_ok property for bucket
     * 
     * @param notFoundOK
     * @return this
     */
    public BucketPropertiesBuilder notFoundOK(boolean notFoundOK) {
        this.notFoundOK = notFoundOK;
        return this;
    }

    /**
     * Enable the bucket for search
     * @param search
     */
    public BucketPropertiesBuilder search(boolean search) {
        this.search = search;
        return this;
    }
}