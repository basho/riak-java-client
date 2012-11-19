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
package com.basho.riak.client.bucket;

import javax.annotation.concurrent.Immutable;

import com.basho.riak.client.cap.Quorum;

/**
 * Parameter wrapper class for the growing number of tunable CAP knobs on Riak
 * 
 * @author russell
 * 
 */
@Immutable
public class TunableCAPProps {

    private final Quorum r;
    private final Quorum w;
    private final Quorum dw;
    private final Quorum rw;
    private final Quorum pr;
    private final Quorum pw;
    private final Boolean basicQuorum;
    private final Boolean notFoundOK;

    /**
     * Create an instance to wrap the given set of CAP properties
     * 
     * @param r
     * @param w
     * @param dw
     * @param rw
     * @param pr
     * @param pw
     * @param basicQuorum
     * @param notFoundOK
     * @see BucketProperties for meaning of these properties
     */
    public TunableCAPProps(Quorum r, Quorum w, Quorum dw, Quorum rw, Quorum pr, Quorum pw, Boolean basicQuorum,
            Boolean notFoundOK) {
        this.r = r;
        this.w = w;
        this.dw = dw;
        this.rw = rw;
        this.pr = pr;
        this.pw = pw;
        this.basicQuorum = basicQuorum;
        this.notFoundOK = notFoundOK;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#getR()
     */
    public Quorum getR() {
        return r;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#getW()
     */
    public Quorum getW() {
        return w;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#getRW()
     */
    public Quorum getRW() {
        return rw;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#getDW()
     */
    public Quorum getDW() {
        return dw;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#getPR()
     */
    public Quorum getPR() {
        return pr;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#getPW()
     */
    public Quorum getPW() {
        return pw;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#getBasicQuorum()
     */
    public Boolean getBasicQuorum() {
        return basicQuorum;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.bucket.BucketProperties#getNotFoundOK()
     */
    public Boolean getNotFoundOK() {
        return notFoundOK;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((basicQuorum == null) ? 0 : basicQuorum.hashCode());
        result = prime * result + ((dw == null) ? 0 : dw.hashCode());
        result = prime * result + ((notFoundOK == null) ? 0 : notFoundOK.hashCode());
        result = prime * result + ((pr == null) ? 0 : pr.hashCode());
        result = prime * result + ((pw == null) ? 0 : pw.hashCode());
        result = prime * result + ((r == null) ? 0 : r.hashCode());
        result = prime * result + ((rw == null) ? 0 : rw.hashCode());
        result = prime * result + ((w == null) ? 0 : w.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof TunableCAPProps)) {
            return false;
        }
        TunableCAPProps other = (TunableCAPProps) obj;
        if (basicQuorum == null) {
            if (other.basicQuorum != null) {
                return false;
            }
        } else if (!basicQuorum.equals(other.basicQuorum)) {
            return false;
        }
        if (dw == null) {
            if (other.dw != null) {
                return false;
            }
        } else if (!dw.equals(other.dw)) {
            return false;
        }
        if (notFoundOK == null) {
            if (other.notFoundOK != null) {
                return false;
            }
        } else if (!notFoundOK.equals(other.notFoundOK)) {
            return false;
        }
        if (pr == null) {
            if (other.pr != null) {
                return false;
            }
        } else if (!pr.equals(other.pr)) {
            return false;
        }
        if (pw == null) {
            if (other.pw != null) {
                return false;
            }
        } else if (!pw.equals(other.pw)) {
            return false;
        }
        if (r == null) {
            if (other.r != null) {
                return false;
            }
        } else if (!r.equals(other.r)) {
            return false;
        }
        if (rw == null) {
            if (other.rw != null) {
                return false;
            }
        } else if (!rw.equals(other.rw)) {
            return false;
        }
        if (w == null) {
            if (other.w != null) {
                return false;
            }
        } else if (!w.equals(other.w)) {
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override public String toString() {
        return String.format("TunableCAPProps [r=%s, w=%s, dw=%s, rw=%s, pr=%s, pw=%s, basicQuorum=%s, notFoundOK=%s]",
                             r, w, dw, rw, pr, pw, basicQuorum, notFoundOK);
    }
}
