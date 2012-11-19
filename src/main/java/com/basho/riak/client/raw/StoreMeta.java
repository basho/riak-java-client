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

import javax.annotation.concurrent.ThreadSafe;

/**
 * Encapsulates the optional parameters for a store operation on Riak
 * 
 * @author russell
 * @see RawClient#store(com.basho.riak.client.IRiakObject, StoreMeta)
 */
@ThreadSafe
public class StoreMeta {

    private static final StoreMeta EMPTY = new StoreMeta(null, null, null, false, null, null);

    private final Quorum w;
    private final Quorum dw;
    private final Quorum pw;
    private final Boolean returnBody;
    private final Boolean returnHead;
    private final Boolean ifNoneMatch;
    private final Boolean ifNotModified;
    // these two are HTTP API specific for ifNoneMatch and ifNotModified
    // which are different to the PB options of the same name
    private String[] etags;
    private Long lastModified;

    /**
     * Create a StoreMeta, accepts <code>null</code>s for any parameter
     * 
     * @param w
     *            the write quorum for a store operation
     * @param dw
     *            the durable write quorum for a store operation
     * @param pw
     *            the primary write quorum
     * @param returnBody
     *            should the store operation return the new data item and its
     *            meta data
     * @param ifNoneMatch
     *            only store if bucket/key does not exist
     * @param ifNotModified
     *            only store is the vclock supplied on store matches the vclock
     *            in Riak
     */
    public StoreMeta(Integer w, Integer dw, Integer pw, Boolean returnBody, Boolean ifNoneMatch, Boolean ifNotModified) {
        this(null == w ? null : new Quorum(w), 
             null == dw ? null : new Quorum(dw), 
             null == pw ? null : new Quorum(pw), 
             returnBody, 
             null, 
             ifNoneMatch, 
             ifNotModified
            );
    }

    /**
     * Create a StoreMeta, accepts <code>null</code>s for any parameter
     * 
     * @param w
     *            the write quorum for a store operation
     * @param dw
     *            the durable write quorum for a store operation
     * @param pw
     *            the primary write quorum
     * @param returnBody
     *            should the store operation return the new data item and its
     *            meta data
     * @param returnHead
     *            should the store operation return only the meta data
     * @param ifNoneMatch
     *            only store if bucket/key does not exist
     * @param ifNotModified
     *            only store is the vclock supplied on store matches the vclock
     *            in Riak
     */
    public StoreMeta(Integer w, Integer dw, Integer pw, Boolean returnBody, Boolean returnHead, Boolean ifNoneMatch,
                     Boolean ifNotModified) {
        this(null == w ? null : new Quorum(w), 
             null == dw ? null : new Quorum(dw), 
             null == pw ? null : new Quorum(pw), 
             returnBody, 
             returnHead, 
             ifNoneMatch, 
             ifNotModified
        );
    }
    
    /**
     * Create a StoreMeta, accepts <code>null</code>s for any parameter
     * 
     * @param w
     *            the write quorum for a store operation
     * @param dw
     *            the durable write quorum for a store operation
     * @param pw
     *            the primary write quorum
     * @param returnBody
     *            should the store operation return the new data item and its
     *            meta data
     * @param returnHead
     *            should the store operation return only the meta data
     * @param ifNoneMatch
     *            only store if bucket/key does not exist
     * @param ifNotModified
     *            only store is the vclock supplied on store matches the vclock
     *            in Riak
     */
    public StoreMeta(Quorum w, Quorum dw, Quorum pw, Boolean returnBody, Boolean returnHead, Boolean ifNoneMatch,
            Boolean ifNotModified) {
        this.w = w;
        this.dw = dw;
        this.pw = pw;
        this.returnBody = returnBody;
        this.returnHead = returnHead;
        this.ifNoneMatch = ifNoneMatch;
        this.ifNotModified = ifNotModified;
    }

    /**
     * The write quorum
     * @return an Integer or null if no write quorum set
     */
    public Quorum getW() {
        return w;
    }

    /**
     * Is the write quorum set?
     * @return <code>true</code> if the write quorum is set, <code>false</code> otherwise
     */
    public boolean hasW() {
        return w != null;
    }

    /**
     * The durable write quorum
     * @return an Integer or <code>null</code> if the durable write quorum is not set
     */
    public Quorum getDw() {
        return dw;
    }

    /**
     * Has the durable write quorum been set?
     * @return <code>true</code> if durable write quorum is set, <code>false</code> otherwise
     */
    public boolean hasDw() {
        return dw != null;
    }

    /**
     * Has the return body parameter been set?
     * @return <code>true</code> of return body parameter is set, <code>false</code> otherwise
     */
    public boolean hasReturnBody() {
        return returnBody != null;
    }

    /**
     * Get the value for the return body parameter
     * @return the returnBody parameter or <code>null</code> if it is not set
     */
    public Boolean getReturnBody() {
        return returnBody;
    }

    /**
     * Has the pw parameter been set?
     * 
     * @return <code>true</code> if pw parameter is set, <code>false</code>
     *         otherwise
     */
    public boolean hasPw() {
        return pw != null;
    }

    /**
     * Get the value for the pw parameter
     * 
     * @return the pw or <code>null</code> if it is not set.
     */
    public Quorum getPw() {
        return pw;
    }

    /**
     * Has the ifNoneMatch parameter been set?
     * 
     * @return <code>true</code> if ifNoneMatch parameter is set,
     *         <code>false</code> otherwise
     */
    public boolean hasIfNoneMatch() {
        return ifNoneMatch != null;
    }

    /**
     * Get the value of the ifNoneMatch parameter
     * 
     * @return the ifNoneMatch or <code>null</code> if not set.
     */
    public Boolean getIfNoneMatch() {
        return ifNoneMatch;
    }

    /**
     * @return true if hasIfNoneMatch && getIfNoneMatch
     */
    public boolean isIfNoneMatch() {
        return hasIfNoneMatch() && ifNoneMatch;
    }

    /**
     * Has the ifNotModified parameter been set?
     * 
     * @return <code>true</code> if ifNotModified parameter is set,
     *         <code>false</code> otherwise
     */
    public boolean hasIfNotModified() {
        return ifNotModified != null;
    }

    /**
     * Get the value of the ifNoneMatch parameter
     * 
     * @return the ifNotModified or <code>null</code> if not set.
     */
    public Boolean getIfNotModified() {
        return ifNotModified;
    }

    /**
     * @return true if hasIfNotModified && getIfNotModified, false otherwise
     */
    public boolean isIfNotModified() {
        return hasIfNotModified() && ifNotModified;
    }

    /**
     * @return an empty StoreMeta
     */
    public static StoreMeta empty() {
        return EMPTY;
    }

    /**
     * @return true if returnHead is set, false otherwise
     */
    public boolean hasReturnHead() {
        return returnHead != null;
    }

    /**
     * @return the returnHead value or null (if not set)
     */
    public Boolean getReturnHead() {
        return returnHead;
    }

    /**
     * Optional supporting data for ifNoneMatch for the HTTP API
     * 
     * @param etags
     *            the array of etags to provide for ifNoneMatch
     * @return this.
     */
    public synchronized StoreMeta etags(String[] etags) {
        if (etags != null) {
            this.etags = etags.clone();
        }
        return this;
    }

    /**
     * @return the etags
     */
    public synchronized String[] getEtags() {
        if (etags != null) {
            return etags.clone();
        }
        return null;
    }

    /**
     * @return the lastModified
     */
    public synchronized Date getLastModified() {
        if (lastModified != null) {
            return new Date(lastModified);
        }
        return null;
    }

    /**
     * Optional supporting parameter for ifNotModified for the HTTP API
     * 
     * @param lastModified
     *            a Date
     * @return this
     */
    public synchronized StoreMeta lastModified(Date lastModified) {
        if (lastModified != null) {
            this.lastModified = lastModified.getTime();
        }

        return this;
    }

    /**
     * @return a StoreMeta with only the headOnly set to true
     */
    public static StoreMeta headOnly() {
        return new StoreMeta((Quorum)null, null, null, null, true, null, null);
    }

    public static class Builder {
        private Quorum w;
        private Quorum dw;
        private Quorum pw;
        private Boolean returnBody;
        private Boolean returnHead;
        private Boolean ifNotModified;
        private Boolean ifNoneMatch;

        public StoreMeta build() {
            return new StoreMeta(w, dw, pw, returnBody, returnHead, ifNoneMatch, ifNotModified);
        }

        public Builder w(int w) {
            this.w = new Quorum(w);
            return this;
        }

        public Builder w(Quora w) {
            this.w = new Quorum(w);
            return this;
        }
        
        public Builder w(Quorum w) {
            this.w = w;
            return this;
        }
        
        public Builder dw(int dw) {
            this.dw = new Quorum(dw);
            return this;
        }

        public Builder dw(Quora dw) {
            this.dw = new Quorum(dw);
            return this;
        }
        
        public Builder dw(Quorum dw) {
            this.dw = dw;
            return this;
        }
        
        public Builder pw(int pw) {
            this.pw = new Quorum(pw);
            return this;
        }

        public Builder pw(Quora pw) {
            this.pw = new Quorum(pw);
            return this;
        }
        
        public Builder pw(Quorum pw) {
            this.pw = pw;
            return this;
        }
        
        public Builder returnBody(boolean returnBody) {
            this.returnBody = returnBody;
            return this;
        }

        public Builder returnHead(boolean returnHead) {
            this.returnHead = returnHead;
            return this;
        }

        public Builder ifNotModified(boolean ifNotModified) {
            this.ifNotModified = ifNotModified;
            return this;
        }

        public Builder ifNoneMatch(boolean ifNoneMatch) {
            this.ifNoneMatch = ifNoneMatch;
            return this;
        }
    }
}
