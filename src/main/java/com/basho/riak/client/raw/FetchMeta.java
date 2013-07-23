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
package com.basho.riak.client.raw;

import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;
import java.util.Date;

import com.basho.riak.client.cap.VClock;

/**
 * Encapsulates the set of parameters available when fetching data from Riak
 * 
 * @author russell
 * 
 */
public class FetchMeta {

    private final Quorum r;
    private final Quorum pr;
    private final Boolean notFoundOK;
    private final Boolean basicQuorum;
    private final Boolean headOnly;
    private final Boolean returnDeletedVClock;
    private final VClock ifModifiedVClock;
    private final Date ifModifiedSince;
    private final Integer timeout;

    /**
     * Create a fetch meta with the specified parameters for a conditional fetch
     * with the either API
     * 
     * @param r
     *            how many vnodes must reply
     * @param pr
     *            how many primary vnodes must reply, takes precedence over r
     * @param notFoundOK
     *            if a notfound response counts towards satisfying the r value
     * @param basicQuorum
     *            if after a quorum of notfounds/error return at once
     * @param headOnly
     *            only return the object meta, not its value
     * @param returnDeletedVClock
     *            if an object has been deleted, return the tombstone vclock
     * @param ifModifiedSince
     *            a date for conditional get. Not null value means only return a
     *            value if the last_modified date is later than this date *NOTE*
     *            only for HTTP API!!!
     * @param ifModifiedVClock
     *            a vclock for conditional get. Not null value means only return
     *            a value if the current vclock does not match this one. *NOTE*
     *            Only for PB API!
     */
    public FetchMeta(Integer r, Integer pr, Boolean notFoundOK, Boolean basicQuorum, Boolean headOnly,
            Boolean returnDeletedVClock, Date ifModifiedSince, VClock ifModifiedVClock, Integer timeout) {
        
        // A lot of the old code depends on r and pr being returned as null if
        // they aren't set / passed in as null
        this( (null == r ? null : new Quorum(r)), 
              (null == pr ? null : new Quorum(pr)),
              notFoundOK,
              basicQuorum,
              headOnly,
              returnDeletedVClock,
              ifModifiedSince,
              ifModifiedVClock,
              timeout
            );
        
    }

    /**
     * Create a fetch meta with the specified parameters for a conditional fetch
     * with the either API
     * 
     * @param r
     *            how many vnodes must reply
     * @param pr
     *            how many primary vnodes must reply, takes precedence over r
     * @param notFoundOK
     *            if a notfound response counts towards satisfying the r value
     * @param basicQuorum
     *            if after a quorum of notfounds/error return at once
     * @param headOnly
     *            only return the object meta, not its value
     * @param returnDeletedVClock
     *            if an object has been deleted, return the tombstone vclock
     * @param ifModifiedSince
     *            a date for conditional get. Not null value means only return a
     *            value if the last_modified date is later than this date *NOTE*
     *            only for HTTP API!!!
     * @param ifModifiedVClock
     *            a vclock for conditional get. Not null value means only return
     *            a value if the current vclock does not match this one. *NOTE*
     *            Only for PB API!
     */
    public FetchMeta(Quorum r, Quorum pr, Boolean notFoundOK, Boolean basicQuorum, Boolean headOnly,
            Boolean returnDeletedVClock, Date ifModifiedSince, VClock ifModifiedVClock, Integer timeout) {
        
        this.r = r;
        this.pr = pr;
        
        this.notFoundOK = notFoundOK;
        this.basicQuorum = basicQuorum;
        this.headOnly = headOnly;
        this.returnDeletedVClock = returnDeletedVClock;
        this.ifModifiedVClock = ifModifiedVClock;
        this.ifModifiedSince = ifModifiedSince;
        this.timeout = timeout;
    }
    
    /**
     * @return true if the r parameter is set, false otherwise
     */
    public boolean hasR() {
        return r != null;
    }

    /**
     * @return the r
     */
    public Quorum getR() {
        return r;
    }

    /**
     * @return true if the pr parameter is set, false otherwise
     */
    public boolean hasPr() {
        return pr != null;
    }
    /**
     * @return the pr
     */
    public Quorum getPr() {
        return pr;
    }

    /**
     * @return true if the notFoundOk parameter is set, false otherwise
     */
    public boolean hasNotFoundOk() {
        return notFoundOK != null;
    }

    /**
     * @return the notFoundOK
     */
    public Boolean getNotFoundOK() {
        return notFoundOK;
    }

    /**
     * @return true if the basicQuorum parameter is set, false otherwise
     */
    public boolean hasBasicQuorum() {
        return basicQuorum != null;
    }

    /**
     * @return the basicQuorum
     */
    public Boolean getBasicQuorum() {
        return basicQuorum;
    }

    /**
     * @return true if the headOnly parameter is set, false otherwise
     */
    public boolean hasHeadOnly() {
        return headOnly != null;
    }

    /**
     * @return the headOnly
     */
    public Boolean getHeadOnly() {
        return headOnly;
    }

    /**
     * @return true if the returnDeletedVClock parameter is set, false otherwise
     */
    public boolean hasReturnDeletedVClock() {
        return returnDeletedVClock != null;
    }

    /**
     * @return the returnDeletedVClock
     */
    public Boolean getReturnDeletedVClock() {
        return returnDeletedVClock;
    }

    /**
     * The {@link VClock} to use in a conditional fetch with the PB API.
     * 
     * @return the {@link VClock} value, null means this fetch is not
     *         conditional.
     */
    public VClock getIfModifiedVClock() {
        return ifModifiedVClock;
    }

    /**
     * The date for an HTTP fetch if-modified-since.
     * 
     * @return the Date value, null means this fetch is not conditional.
     */
    public Date getIfModifiedSince() {
        return ifModifiedSince;
    }

    /**
     * Returns true if the timeout parameter is set, otherwise false
     * @return if the timeout is set or not
     */
    public boolean hasTimeout() {
        return timeout != null;
    }
    
    /**
     * Returns the timeout value if set, otherwise null
     * @return the timeout in milliseconds
     */
    public Integer getTimeout() {
        return timeout;
    }
    
    /**
     * Convenient way to create a fetch meta with just an r value
     * 
     * @param readQuorum
     * @return a {@link FetchMeta} with just an R value
     */
    public static FetchMeta withR(int readQuorum) {
        return new FetchMeta(readQuorum, null, null, null, null, null, null, null, null);
    }

    // Builder
    public static class Builder {
        private Quorum r;
        private Quorum pr;
        private Boolean notFoundOK;
        private Boolean basicQuorum;
        private Boolean headOnly;
        private Boolean returnDeletedVClock;
        private VClock vclock;
        private Date modifiedSince;
        private Integer timeout;

        public static Builder from(FetchMeta fm) {
            Builder b = new Builder();
            b.r = fm.getR();
            b.pr = fm.getPr();
            b.notFoundOK = fm.getNotFoundOK();
            b.basicQuorum = fm.getBasicQuorum();
            b.returnDeletedVClock = fm.getReturnDeletedVClock();
            b.vclock = fm.getIfModifiedVClock();
            b.modifiedSince = fm.getIfModifiedSince();
            return b;
        }

        public FetchMeta build() {
            return new FetchMeta(r, pr, notFoundOK, basicQuorum, headOnly, returnDeletedVClock, modifiedSince, vclock, timeout);
        }

        public Builder r(int r) {
            this.r = new Quorum(r);
            return this;
        }

        public Builder r(Quora r) {
            this.r = new Quorum(r);
            return this;
        }
        
        public Builder r(Quorum r) {
            this.r = r;
            return this;
        }
        
        public Builder pr(int pr) {
            this.pr = new Quorum(pr);
            return this;
        }

        public Builder pr(Quora pr) {
            this.pr = new Quorum(pr);
            return this;
        }
        
        public Builder pr(Quorum pr) {
            this.pr = pr;
            return this;
        }
        
        public Builder notFoundOK(boolean notFoundOK) {
            this.notFoundOK = notFoundOK;
            return this;
        }

        public Builder basicQuorum(boolean basicQuorum) {
            this.basicQuorum = basicQuorum;
            return this;
        }

        public Builder returnDeletedVClock(boolean returnDeletedVClock) {
            this.returnDeletedVClock = returnDeletedVClock;
            return this;
        }

        public Builder headOnly(boolean headOnly) {
            this.headOnly = headOnly;
            return this;
        }

        public Builder vclock(VClock vclock) {
            this.vclock = vclock;
            return this;
        }

        public Builder modifiedSince(Date modifiedSince) {
            this.modifiedSince = modifiedSince;
            return this;
        }
        
        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }
    }

    /**
     * @return a FetchMeta empty for everything except <code>headOnly</code>
     */
    public static FetchMeta head() {
        // Cast first null to Quorum to avoid ambiguous constructor problem
        return new FetchMeta((Quorum)null, null, null, null, true, null, null, null, null);
    }
}
