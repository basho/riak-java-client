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

import java.util.Date;

/**
 * Encapsulates the optional parameters for a store operation on Riak
 * 
 * @author russell
 * @see RawClient#store(com.basho.riak.client.IRiakObject, StoreMeta)
 */
public class StoreMeta {

    private static final StoreMeta EMPTY = new StoreMeta(null, null, null, false, null, null);

    private final Integer w;
    private final Integer dw;
    private final Integer pw;
    private final Boolean returnBody;
    private final Boolean ifNonMatch;
    private final Boolean ifNotModified;
    // these two are HTTP API specific for ifNonMatch and ifNotModified
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
     * @param ifNonMatch
     *            only store if bucket/key does not exist
     * @param ifNotModified
     *            only store is the vclock supplied on store matches the vclock
     *            in Riak
     */
    public StoreMeta(Integer w, Integer dw, Integer pw, Boolean returnBody, Boolean ifNonMatch, Boolean ifNotModified) {
        this.w = w;
        this.dw = dw;
        this.pw = pw;
        this.returnBody = returnBody;
        this.ifNonMatch = ifNonMatch;
        this.ifNotModified = ifNotModified;
    }

    /**
     * The write quorum
     * @return an Integer or null if no write quorum set
     */
    public Integer getW() {
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
    public Integer getDw() {
        return dw;
    }

    /**
     * Has the durable write quorum been set?
     * @return <code>true</code> if durable write quorum is set, <code>false</code> otherwise
     */
    public boolean hasDW() {
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
    public Integer getPw() {
        return pw;
    }

    /**
     * Has the ifNonMatch parameter been set?
     * 
     * @return <code>true</code> if ifNonMatch parameter is set,
     *         <code>false</code> otherwise
     */
    public boolean hasIfNonMatch() {
        return ifNonMatch != null;
    }

    /**
     * Get the value of the ifNonMatch parameter
     * 
     * @return the ifNonMatch or <code>null</code> if not set.
     */
    public Boolean getIfNonMatch() {
        return ifNonMatch;
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
     * Get the value of the ifNonMatch parameter
     * 
     * @return the ifNotModified or <code>null</code> if not set.
     */
    public Boolean getIfNotModified() {
        return ifNotModified;
    }

    /**
     * @return
     */
    public static StoreMeta empty() {
        return EMPTY;
    }

    /**
     * Optional supporting data for ifNonMatch for the HTTP API
     * 
     * @param etags
     *            the array of etags to provide for ifNonMatch
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
}
